// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/codegen/artifact_codec.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <new>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"

namespace mino::schema::codegen {
namespace {

constexpr std::string_view kMagic = "MINODSC2";
constexpr size_t kMaxArtifactBytes = 16u << 20;
constexpr uint32_t kMaxTypes = 4096;
constexpr uint32_t kMaxFields = 4096;
constexpr uint32_t kMaxDependencies = 4096;
constexpr uint32_t kMaxReserved = 4096;
constexpr uint32_t kMaxStringBytes = 4u << 20;
constexpr size_t kMaxTypeDepth = 32;
constexpr uint64_t kNoOffset = std::numeric_limits<uint64_t>::max();

Status Invalid(std::string_view message) {
    return Status::Error(StatusCode::kInvalidArgument, message);
}

class Writer {
public:
    void Byte(uint8_t value) { bytes_.push_back(static_cast<char>(value)); }

    void U32(uint32_t value) {
        for (size_t i = 0; i < 4; ++i) Byte(static_cast<uint8_t>(value >> (i * 8)));
    }

    void U64(uint64_t value) {
        for (size_t i = 0; i < 8; ++i) Byte(static_cast<uint8_t>(value >> (i * 8)));
    }

    void Bytes(std::string_view value) { bytes_.append(value); }

    void String(std::string_view value) {
        U32(static_cast<uint32_t>(value.size()));
        Bytes(value);
    }

    void Digest(const CanonicalDigest& digest) {
        for (std::byte byte : digest) Byte(static_cast<uint8_t>(byte));
    }

    std::string Finish() && { return std::move(bytes_); }

private:
    std::string bytes_;
};

class Reader {
public:
    explicit Reader(std::string_view bytes) : bytes_(bytes) {}

    Result<uint8_t> Byte() {
        if (offset_ == bytes_.size()) return Invalid("descriptor artifact is truncated");
        return static_cast<uint8_t>(bytes_[offset_++]);
    }

    Result<uint32_t> U32() {
        if (!Available(4)) return Invalid("descriptor artifact is truncated");
        uint32_t value = 0;
        for (size_t i = 0; i < 4; ++i) {
            value |= static_cast<uint32_t>(
                         static_cast<uint8_t>(bytes_[offset_ + i]))
                     << (i * 8);
        }
        offset_ += 4;
        return value;
    }

    Result<uint64_t> U64() {
        if (!Available(8)) return Invalid("descriptor artifact is truncated");
        uint64_t value = 0;
        for (size_t i = 0; i < 8; ++i) {
            value |= static_cast<uint64_t>(
                         static_cast<uint8_t>(bytes_[offset_ + i]))
                     << (i * 8);
        }
        offset_ += 8;
        return value;
    }

    Result<std::string> String() {
        auto length = U32();
        if (!length.ok()) return length.status();
        if (*length > kMaxStringBytes || !Available(*length)) {
            return Invalid("descriptor string length is invalid");
        }
        std::string value(bytes_.substr(offset_, *length));
        offset_ += *length;
        return value;
    }

    Result<CanonicalDigest> Digest() {
        if (!Available(32)) return Invalid("descriptor digest is truncated");
        CanonicalDigest digest{};
        for (size_t i = 0; i < digest.size(); ++i) {
            digest[i] = static_cast<std::byte>(
                static_cast<uint8_t>(bytes_[offset_ + i]));
        }
        offset_ += digest.size();
        return digest;
    }

    bool done() const noexcept { return offset_ == bytes_.size(); }

private:
    bool Available(size_t bytes) const noexcept {
        return offset_ <= bytes_.size() && bytes <= bytes_.size() - offset_;
    }

    std::string_view bytes_;
    size_t offset_ = 0;
};

uint8_t ScalarCode(ScalarType scalar) {
    switch (scalar) {
        case ScalarType::kInt32: return 1;
        case ScalarType::kInt64: return 2;
        case ScalarType::kUint32: return 3;
        case ScalarType::kUint64: return 4;
        case ScalarType::kFixed32: return 5;
        case ScalarType::kFixed64: return 6;
        case ScalarType::kFloat: return 7;
        case ScalarType::kDouble: return 8;
        case ScalarType::kBool: return 9;
        case ScalarType::kString: return 10;
        case ScalarType::kBytes: return 11;
    }
    return 0;
}

Result<ScalarType> DecodeScalar(uint8_t code) {
    switch (code) {
        case 1: return ScalarType::kInt32;
        case 2: return ScalarType::kInt64;
        case 3: return ScalarType::kUint32;
        case 4: return ScalarType::kUint64;
        case 5: return ScalarType::kFixed32;
        case 6: return ScalarType::kFixed64;
        case 7: return ScalarType::kFloat;
        case 8: return ScalarType::kDouble;
        case 9: return ScalarType::kBool;
        case 10: return ScalarType::kString;
        case 11: return ScalarType::kBytes;
        default: return Invalid("descriptor has an invalid scalar code");
    }
}

void EncodeType(const TypeDescriptor& type, Writer& writer) {
    switch (type.kind()) {
        case TypeDescriptor::Kind::kScalar:
            writer.Byte(1);
            writer.Byte(ScalarCode(*type.scalar()));
            return;
        case TypeDescriptor::Kind::kUserDefined:
            writer.Byte(2);
            writer.String(type.name());
            return;
        case TypeDescriptor::Kind::kVector:
            writer.Byte(3);
            EncodeType(*type.element_type(), writer);
            return;
    }
}

Result<TypeDescriptor> DecodeType(Reader& reader, size_t depth) {
    if (depth > kMaxTypeDepth) return Invalid("descriptor type nesting is too deep");
    auto kind = reader.Byte();
    if (!kind.ok()) return kind.status();
    if (*kind == 1) {
        auto code = reader.Byte();
        if (!code.ok()) return code.status();
        auto scalar = DecodeScalar(*code);
        if (!scalar.ok()) return scalar.status();
        static constexpr std::array<std::string_view, 11> kNames = {
            "int32", "int64", "uint32", "uint64", "fixed32", "fixed64",
            "float", "double", "bool", "string", "bytes"};
        return TypeDescriptor::Scalar(*scalar, std::string(kNames[*code - 1]));
    }
    if (*kind == 2) {
        auto name = reader.String();
        if (!name.ok()) return name.status();
        if (name->empty()) return Invalid("descriptor user type name is empty");
        return TypeDescriptor::UserDefined(std::move(*name));
    }
    if (*kind == 3) {
        auto element = DecodeType(reader, depth + 1);
        if (!element.ok()) return element.status();
        return TypeDescriptor::Vector(std::move(*element));
    }
    return Invalid("descriptor has an invalid type kind");
}

uint8_t DefaultCode(DefaultValue::Kind kind) {
    switch (kind) {
        case DefaultValue::Kind::kInteger: return 1;
        case DefaultValue::Kind::kFloat32: return 2;
        case DefaultValue::Kind::kFloat64: return 3;
        case DefaultValue::Kind::kBoolean: return 4;
        case DefaultValue::Kind::kString: return 5;
        case DefaultValue::Kind::kBytes: return 6;
    }
    return 0;
}

Result<DefaultValue::Kind> DecodeDefaultKind(uint8_t code) {
    switch (code) {
        case 1: return DefaultValue::Kind::kInteger;
        case 2: return DefaultValue::Kind::kFloat32;
        case 3: return DefaultValue::Kind::kFloat64;
        case 4: return DefaultValue::Kind::kBoolean;
        case 5: return DefaultValue::Kind::kString;
        case 6: return DefaultValue::Kind::kBytes;
        default: return Invalid("descriptor has an invalid default kind");
    }
}

uint8_t CardinalityCode(FieldCardinality cardinality) {
    switch (cardinality) {
        case FieldCardinality::kUnspecified: return 0;
        case FieldCardinality::kOptional: return 1;
        case FieldCardinality::kRequired: return 2;
    }
    return 0xff;
}

Result<FieldCardinality> DecodeCardinality(uint8_t code) {
    switch (code) {
        case 0: return FieldCardinality::kUnspecified;
        case 1: return FieldCardinality::kOptional;
        case 2: return FieldCardinality::kRequired;
        default: return Invalid("descriptor has invalid field cardinality");
    }
}

uint8_t StorageCode(FieldStorageKind storage) {
    switch (storage) {
        case FieldStorageKind::kScalar: return 1;
        case FieldStorageKind::kInlineStruct: return 2;
        case FieldStorageKind::kVariable: return 3;
    }
    return 0;
}

Result<FieldStorageKind> DecodeStorage(uint8_t code) {
    switch (code) {
        case 1: return FieldStorageKind::kScalar;
        case 2: return FieldStorageKind::kInlineStruct;
        case 3: return FieldStorageKind::kVariable;
        default: return Invalid("descriptor has invalid storage kind");
    }
}

void EncodeLayout(const LayoutPlan& layout, Writer& writer) {
    writer.U32(layout.layout_version());
    writer.U64(layout.header_size());
    writer.U64(layout.presence_bitmap_offset());
    writer.U64(layout.presence_bitmap_words());
    writer.U64(layout.fixed_area_offset());
    writer.U64(layout.fixed_area_size());
    writer.U64(layout.unknown_fields_offset().has_value()
                   ? *layout.unknown_fields_offset()
                   : kNoOffset);
    writer.U64(layout.object_size());
    writer.U64(layout.object_alignment());
    writer.U64(layout.max_child_bytes());
    writer.U64(layout.max_dynamic_children());
}



bool PowerOfTwo(size_t value) noexcept {
    return value != 0 && (value & (value - 1)) == 0;
}

size_t ScalarLayoutSize(ScalarType scalar) noexcept {
    switch (scalar) {
        case ScalarType::kBool: return 1;
        case ScalarType::kInt32:
        case ScalarType::kUint32:
        case ScalarType::kFixed32:
        case ScalarType::kFloat: return 4;
        case ScalarType::kInt64:
        case ScalarType::kUint64:
        case ScalarType::kFixed64:
        case ScalarType::kDouble: return 8;
        case ScalarType::kString:
        case ScalarType::kBytes: return VariableMetadataLayout::kSize;
    }
    return 0;
}

Status ValidateFieldSemantics(const AggregateDescriptor& aggregate) {
    uint32_t previous_id = 0;
    for (const FieldDescriptor& field : aggregate.fields()) {
        if (field.id() <= previous_id || aggregate.IsReserved(field.id())) {
            return Invalid("descriptor field IDs overlap or are not unique");
        }
        previous_id = field.id();
        const TypeDescriptor& type = field.type();
        if (type.kind() == TypeDescriptor::Kind::kScalar) {
            if (!type.scalar().has_value()) return Invalid("scalar type is incomplete");
            const bool bytes = *type.scalar() == ScalarType::kString ||
                               *type.scalar() == ScalarType::kBytes;
            if (bytes != field.constraints().max_bytes().has_value() ||
                field.constraints().max_capacity().has_value()) {
                return Invalid("scalar constraints are inconsistent");
            }
        } else if (type.kind() == TypeDescriptor::Kind::kVector) {
            if (type.element_type() == nullptr ||
                !field.constraints().max_capacity().has_value() ||
                field.constraints().max_bytes().has_value()) {
                return Invalid("vector constraints are inconsistent");
            }
        } else if (field.constraints().max_bytes().has_value() ||
                   field.constraints().max_capacity().has_value()) {
            return Invalid("user-defined field has dynamic scalar constraints");
        }
        if ((field.constraints().max_bytes().has_value() &&
             *field.constraints().max_bytes() == 0) ||
            (field.constraints().max_capacity().has_value() &&
             *field.constraints().max_capacity() == 0)) {
            return Invalid("descriptor constraint must be positive");
        }
        if (field.default_value().has_value()) {
            if (type.kind() != TypeDescriptor::Kind::kScalar) {
                return Invalid("non-scalar descriptor field has a default");
            }
            bool matches = false;
            switch (field.default_value()->kind()) {
                case DefaultValue::Kind::kInteger:
                    matches = *type.scalar() == ScalarType::kInt32 ||
                              *type.scalar() == ScalarType::kInt64 ||
                              *type.scalar() == ScalarType::kUint32 ||
                              *type.scalar() == ScalarType::kUint64 ||
                              *type.scalar() == ScalarType::kFixed32 ||
                              *type.scalar() == ScalarType::kFixed64;
                    break;
                case DefaultValue::Kind::kFloat32:
                    matches = *type.scalar() == ScalarType::kFloat;
                    break;
                case DefaultValue::Kind::kFloat64:
                    matches = *type.scalar() == ScalarType::kDouble;
                    break;
                case DefaultValue::Kind::kBoolean:
                    matches = *type.scalar() == ScalarType::kBool;
                    break;
                case DefaultValue::Kind::kString:
                    matches = *type.scalar() == ScalarType::kString;
                    break;
                case DefaultValue::Kind::kBytes:
                    matches = *type.scalar() == ScalarType::kBytes;
                    break;
            }
            if (!matches) return Invalid("descriptor default kind disagrees with type");
        }
    }
    return Status::Ok();
}

Status ValidateLayoutShape(const SchemaDescriptor& descriptor,
                           const DecodedLayoutArtifact& layout) {
    if (layout.layout_version != descriptor.identity().layout_version() ||
        layout.header_size != ObjectHeaderLayout::kSize ||
        layout.presence_bitmap_offset != ObjectHeaderLayout::kSize ||
        !PowerOfTwo(layout.object_alignment) ||
        layout.object_alignment < ObjectHeaderLayout::kAlignment ||
        layout.object_size == 0 ||
        layout.object_size % layout.object_alignment != 0) {
        return Invalid("descriptor layout header is inconsistent");
    }
    size_t optional_count = 0;
    for (const FieldDescriptor& field : descriptor.aggregate().fields()) {
        if (field.cardinality() == FieldCardinality::kOptional) ++optional_count;
    }
    if (layout.presence_bitmap_words != (optional_count + 63) / 64 ||
        layout.fixed_area_offset < ObjectHeaderLayout::kSize ||
        layout.fixed_area_offset > layout.object_size ||
        layout.fixed_area_size >
            layout.object_size - layout.fixed_area_offset ||
        layout.fields.size() != descriptor.aggregate().fields().size()) {
        return Invalid("descriptor layout dimensions are inconsistent");
    }
    size_t next_presence = 0;
    for (size_t i = 0; i < layout.fields.size(); ++i) {
        const DecodedFieldLayoutArtifact& field_layout = layout.fields[i];
        const FieldDescriptor& field = descriptor.aggregate().fields()[i];
        if (field_layout.field_id != field.id() ||
            !PowerOfTwo(field_layout.alignment) ||
            field_layout.offset % field_layout.alignment != 0 ||
            field_layout.offset > layout.object_size ||
            field_layout.size > layout.object_size - field_layout.offset) {
            return Invalid("descriptor field layout is inconsistent");
        }
        if (field.type().kind() == TypeDescriptor::Kind::kScalar) {
            const size_t expected_size = ScalarLayoutSize(*field.type().scalar());
            const FieldStorageKind expected_storage =
                (*field.type().scalar() == ScalarType::kString ||
                 *field.type().scalar() == ScalarType::kBytes)
                    ? FieldStorageKind::kVariable
                    : FieldStorageKind::kScalar;
            if (field_layout.size != expected_size ||
                field_layout.storage_kind != expected_storage) {
                return Invalid("scalar descriptor layout has wrong shape");
            }
        } else if (field.type().kind() == TypeDescriptor::Kind::kVector &&
                   (field_layout.size != VariableMetadataLayout::kSize ||
                    field_layout.storage_kind != FieldStorageKind::kVariable)) {
            return Invalid("vector descriptor layout has wrong shape");
        }
        if (field.cardinality() == FieldCardinality::kOptional) {
            if (field_layout.presence_bit != next_presence++) {
                return Invalid("descriptor presence bits are not canonical");
            }
        } else if (field_layout.presence_bit.has_value()) {
            return Invalid("non-optional field has a presence bit");
        }
    }
    const bool message = descriptor.aggregate().kind() == AggregateKind::kMessage;
    if (message != layout.unknown_fields_offset.has_value()) {
        return Invalid("descriptor unknown-field layout disagrees with kind");
    }
    if (layout.unknown_fields_offset.has_value() &&
        (*layout.unknown_fields_offset > layout.object_size ||
         VariableMetadataLayout::kSize >
             layout.object_size - *layout.unknown_fields_offset)) {
        return Invalid("descriptor unknown-field metadata is out of bounds");
    }
    return Status::Ok();
}

Status ValidateCanonicalLayout(
    const SchemaDescriptor& descriptor,
    const DecodedLayoutArtifact& layout,
    std::span<const std::shared_ptr<const SchemaDescriptor>> closure) {
    LayoutOptions options;
    options.max_fields = std::max(options.max_fields, layout.fields.size());
    options.max_object_size =
        std::max<uint64_t>(options.max_object_size, layout.object_size);
    options.max_total_child_bytes =
        std::max(options.max_total_child_bytes, layout.max_child_bytes);
    options.max_dynamic_children =
        std::max(options.max_dynamic_children, layout.max_dynamic_children);
    auto expected = LayoutPlanner::Plan(descriptor, closure, options);
    if (!expected.ok()) return expected.status();

    const bool top_level_equal =
        layout.layout_version == expected->layout_version() &&
        layout.header_size == expected->header_size() &&
        layout.presence_bitmap_offset == expected->presence_bitmap_offset() &&
        layout.presence_bitmap_words == expected->presence_bitmap_words() &&
        layout.fixed_area_offset == expected->fixed_area_offset() &&
        layout.fixed_area_size == expected->fixed_area_size() &&
        layout.unknown_fields_offset == expected->unknown_fields_offset() &&
        layout.object_size == expected->object_size() &&
        layout.object_alignment == expected->object_alignment() &&
        layout.max_child_bytes == expected->max_child_bytes() &&
        layout.max_dynamic_children == expected->max_dynamic_children() &&
        layout.fields.size() == expected->fields().size();
    if (!top_level_equal) {
        return Status::Error(StatusCode::kSchemaMismatch,
                             "descriptor layout is not canonical");
    }
    for (size_t i = 0; i < layout.fields.size(); ++i) {
        const DecodedFieldLayoutArtifact& actual = layout.fields[i];
        const FieldLayout& wanted = expected->fields()[i];
        if (actual.field_id != wanted.field_id() ||
            actual.offset != wanted.offset() || actual.size != wanted.size() ||
            actual.alignment != wanted.alignment() ||
            actual.storage_kind != wanted.storage_kind() ||
            actual.presence_bit != wanted.presence_bit() ||
            actual.max_child_bytes != wanted.max_child_bytes() ||
            actual.max_dynamic_children != wanted.max_dynamic_children()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "descriptor field layout is not canonical");
        }
    }
    return Status::Ok();
}

}  // namespace

Result<std::string> EncodeDescriptorArtifact(
    const CompiledSchema& schema,
    std::span<const LayoutPlan> layouts) noexcept {
    try {
        if (schema.types().size() != layouts.size() ||
            schema.types().size() > kMaxTypes) {
            return Invalid("descriptor encode input counts are invalid");
        }
        Writer writer;
        writer.Bytes(kMagic);
        writer.U32(kDescriptorArtifactVersion);
        writer.U32(static_cast<uint32_t>(schema.types().size()));
        for (size_t i = 0; i < schema.types().size(); ++i) {
            if (schema.types()[i] == nullptr) {
                return Invalid("descriptor encode input contains null type");
            }
            const SchemaDescriptor& descriptor = *schema.types()[i];
            const AggregateDescriptor& aggregate = descriptor.aggregate();
            const LayoutPlan& layout = layouts[i];
            if (aggregate.fields().size() > kMaxFields ||
                aggregate.reserved_ranges().size() > kMaxReserved ||
                descriptor.dependencies().size() > kMaxDependencies) {
                return Invalid("descriptor encode collection exceeds limit");
            }
            writer.String(aggregate.full_name());
            writer.Byte(aggregate.kind() == AggregateKind::kMessage ? 1 : 2);
            writer.U64(descriptor.identity().short_id());
            writer.Digest(descriptor.identity().canonical_digest());
            writer.U32(descriptor.identity().schema_version());
            writer.U32(descriptor.identity().layout_version());
            EncodeLayout(layout, writer);

            writer.U32(static_cast<uint32_t>(descriptor.dependencies().size()));
            for (const DependencyDescriptor& dependency : descriptor.dependencies()) {
                writer.String(dependency.full_name());
                writer.Digest(dependency.digest());
            }
            writer.U32(static_cast<uint32_t>(aggregate.reserved_ranges().size()));
            for (const ReservedRangeDescriptor& range : aggregate.reserved_ranges()) {
                writer.U32(range.first());
                writer.U32(range.last());
            }
            writer.U32(static_cast<uint32_t>(aggregate.fields().size()));
            for (size_t field_index = 0; field_index < aggregate.fields().size();
                 ++field_index) {
                const FieldDescriptor& field = aggregate.fields()[field_index];
                const FieldLayout& field_layout = layout.fields()[field_index];
                writer.U32(field.id());
                writer.String(field.name());
                writer.Byte(CardinalityCode(field.cardinality()));
                EncodeType(field.type(), writer);
                uint8_t constraint_flags = 0;
                if (field.constraints().max_bytes().has_value()) constraint_flags |= 1;
                if (field.constraints().max_capacity().has_value()) constraint_flags |= 2;
                if (field.constraints().snapshot_key()) constraint_flags |= 4;
                writer.Byte(constraint_flags);
                if ((constraint_flags & 1) != 0) writer.U64(*field.constraints().max_bytes());
                if ((constraint_flags & 2) != 0) writer.U64(*field.constraints().max_capacity());
                writer.Byte(field.default_value().has_value() ? 1 : 0);
                if (field.default_value().has_value()) {
                    writer.Byte(DefaultCode(field.default_value()->kind()));
                    writer.String(field.default_value()->canonical_value());
                }
                writer.U64(field_layout.offset());
                writer.U64(field_layout.size());
                writer.U64(field_layout.alignment());
                writer.Byte(StorageCode(field_layout.storage_kind()));
                writer.U64(field_layout.presence_bit().has_value()
                               ? *field_layout.presence_bit()
                               : kNoOffset);
                writer.U64(field_layout.max_child_bytes());
                writer.U64(field_layout.max_dynamic_children());
            }
            writer.String(descriptor.canonical_schema());
        }
        std::string result = std::move(writer).Finish();
        const CanonicalDigest artifact_digest = Sha256(result);
        for (std::byte byte : artifact_digest) {
            result.push_back(static_cast<char>(byte));
        }
        if (result.size() > kMaxArtifactBytes) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "descriptor artifact exceeds byte limit");
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<DecodedDescriptorArtifact> DecodeAndValidate(
    std::string_view bytes,
    std::span<const std::shared_ptr<const SchemaDescriptor>>
        external_descriptors) noexcept {
    try {
        if (bytes.size() > kMaxArtifactBytes ||
            bytes.size() < kMagic.size() + 32 || !bytes.starts_with(kMagic)) {
            return Invalid("descriptor artifact magic or size is invalid");
        }
        const std::string_view payload = bytes.substr(0, bytes.size() - 32);
        CanonicalDigest stored_artifact_digest{};
        for (size_t i = 0; i < stored_artifact_digest.size(); ++i) {
            stored_artifact_digest[i] = static_cast<std::byte>(
                static_cast<uint8_t>(bytes[payload.size() + i]));
        }
        if (Sha256(payload) != stored_artifact_digest) {
            return Status::Error(StatusCode::kCorruption,
                                 "descriptor artifact checksum mismatch");
        }
        Reader reader(payload.substr(kMagic.size()));
        auto version = reader.U32();
        auto type_count = reader.U32();
        if (!version.ok() || !type_count.ok() ||
            *version != kDescriptorArtifactVersion || *type_count > kMaxTypes) {
            return Invalid("descriptor artifact version or type count is invalid");
        }
        DecodedDescriptorArtifact result;
        result.version = *version;
        result.types.reserve(*type_count);
        std::map<std::string, CanonicalDigest, std::less<>> identities;
        std::map<uint64_t, CanonicalDigest> short_ids;
        for (uint32_t type_index = 0; type_index < *type_count; ++type_index) {
            auto full_name = reader.String();
            auto kind = reader.Byte();
            auto short_id = reader.U64();
            auto digest = reader.Digest();
            auto schema_version = reader.U32();
            auto layout_version = reader.U32();
            if (!full_name.ok() || !kind.ok() || !short_id.ok() || !digest.ok() ||
                !schema_version.ok() || !layout_version.ok() || full_name->empty() ||
                (*kind != 1 && *kind != 2)) {
                return Invalid("descriptor type identity is invalid");
            }

            auto encoded_layout_version = reader.U32();
            auto header = reader.U64();
            auto presence_offset = reader.U64();
            auto presence_words = reader.U64();
            auto fixed_offset = reader.U64();
            auto fixed_size = reader.U64();
            auto unknown = reader.U64();
            auto object_size = reader.U64();
            auto alignment = reader.U64();
            auto child_bytes = reader.U64();
            auto children = reader.U64();
            if (!encoded_layout_version.ok() || !header.ok() ||
                !presence_offset.ok() || !presence_words.ok() ||
                !fixed_offset.ok() || !fixed_size.ok() || !unknown.ok() ||
                !object_size.ok() || !alignment.ok() || !child_bytes.ok() ||
                !children.ok()) {
                return Invalid("descriptor layout header is truncated");
            }

            auto dependency_count = reader.U32();
            if (!dependency_count.ok() || *dependency_count > kMaxDependencies) {
                return Invalid("descriptor dependency count is invalid");
            }
            std::vector<DependencyDescriptor> dependencies;
            dependencies.reserve(*dependency_count);
            for (uint32_t i = 0; i < *dependency_count; ++i) {
                auto name = reader.String();
                auto dependency_digest = reader.Digest();
                if (!name.ok() || !dependency_digest.ok() || name->empty()) {
                    return Invalid("descriptor dependency is invalid");
                }
                dependencies.emplace_back(std::move(*name), *dependency_digest);
            }

            auto reserved_count = reader.U32();
            if (!reserved_count.ok() || *reserved_count > kMaxReserved) {
                return Invalid("descriptor reserved count is invalid");
            }
            std::vector<ReservedRangeDescriptor> reserved;
            reserved.reserve(*reserved_count);
            for (uint32_t i = 0; i < *reserved_count; ++i) {
                auto first = reader.U32();
                auto last = reader.U32();
                if (!first.ok() || !last.ok() || *first == 0 || *first > *last) {
                    return Invalid("descriptor reserved range is invalid");
                }
                reserved.emplace_back(*first, *last);
            }

            auto field_count = reader.U32();
            if (!field_count.ok() || *field_count > kMaxFields) {
                return Invalid("descriptor field count is invalid");
            }
            std::vector<FieldDescriptor> fields;
            std::vector<DecodedFieldLayoutArtifact> field_layouts;
            fields.reserve(*field_count);
            field_layouts.reserve(*field_count);
            for (uint32_t i = 0; i < *field_count; ++i) {
                auto id = reader.U32();
                auto name = reader.String();
                auto cardinality_code = reader.Byte();
                if (!id.ok() || !name.ok() || !cardinality_code.ok() ||
                    *id == 0 || name->empty()) {
                    return Invalid("descriptor field identity is invalid");
                }
                auto cardinality = DecodeCardinality(*cardinality_code);
                auto type = DecodeType(reader, 0);
                auto constraint_flags = reader.Byte();
                if (!cardinality.ok() || !type.ok() || !constraint_flags.ok() ||
                    (*constraint_flags & ~uint8_t{7}) != 0) {
                    return Invalid("descriptor field semantics are invalid");
                }
                std::optional<uint64_t> max_bytes;
                std::optional<uint64_t> max_capacity;
                if ((*constraint_flags & 1) != 0) {
                    auto value = reader.U64();
                    if (!value.ok()) return value.status();
                    max_bytes = *value;
                }
                if ((*constraint_flags & 2) != 0) {
                    auto value = reader.U64();
                    if (!value.ok()) return value.status();
                    max_capacity = *value;
                }
                auto has_default = reader.Byte();
                if (!has_default.ok() || *has_default > 1) {
                    return Invalid("descriptor default marker is invalid");
                }
                std::optional<DefaultValue> default_value;
                if (*has_default != 0) {
                    auto default_code = reader.Byte();
                    auto value = reader.String();
                    if (!default_code.ok() || !value.ok()) {
                        return Invalid("descriptor default is truncated");
                    }
                    auto default_kind = DecodeDefaultKind(*default_code);
                    if (!default_kind.ok()) return default_kind.status();
                    default_value.emplace(*default_kind, std::move(*value));
                }
                auto offset = reader.U64();
                auto size = reader.U64();
                auto field_alignment = reader.U64();
                auto storage_code = reader.Byte();
                auto presence = reader.U64();
                auto max_field_child_bytes = reader.U64();
                auto max_field_children = reader.U64();
                if (!offset.ok() || !size.ok() || !field_alignment.ok() ||
                    !storage_code.ok() || !presence.ok() ||
                    !max_field_child_bytes.ok() || !max_field_children.ok() ||
                    *offset > std::numeric_limits<size_t>::max() ||
                    *size > std::numeric_limits<size_t>::max() ||
                    *field_alignment > std::numeric_limits<size_t>::max() ||
                    (*presence != kNoOffset &&
                     *presence > std::numeric_limits<size_t>::max())) {
                    return Invalid("descriptor field layout value is invalid");
                }
                auto storage = DecodeStorage(*storage_code);
                if (!storage.ok()) return storage.status();
                fields.emplace_back(
                    *id, std::move(*name), *cardinality, std::move(*type),
                    ConstraintSet(max_bytes, max_capacity,
                                  (*constraint_flags & 4) != 0),
                    std::move(default_value));
                field_layouts.push_back(DecodedFieldLayoutArtifact{
                    .field_id = *id,
                    .offset = static_cast<size_t>(*offset),
                    .size = static_cast<size_t>(*size),
                    .alignment = static_cast<size_t>(*field_alignment),
                    .storage_kind = *storage,
                    .presence_bit = *presence == kNoOffset
                        ? std::nullopt
                        : std::optional<size_t>(static_cast<size_t>(*presence)),
                    .max_child_bytes = *max_field_child_bytes,
                    .max_dynamic_children = *max_field_children,
                });
            }
            auto canonical_bytes = reader.String();
            if (!canonical_bytes.ok()) return canonical_bytes.status();

            AggregateDescriptor aggregate(
                *kind == 1 ? AggregateKind::kMessage : AggregateKind::kStruct,
                *full_name, std::move(fields), std::move(reserved));
            const Status semantic_status = ValidateFieldSemantics(aggregate);
            if (!semantic_status.ok()) return semantic_status;
            auto canonical = Canonicalizer::Canonicalize(aggregate, dependencies);
            if (!canonical.ok() || canonical->text() != *canonical_bytes ||
                canonical->digest() != *digest ||
                canonical->short_id() != *short_id ||
                DigestShortId(*digest) != *short_id) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "descriptor canonical identity validation failed");
            }
            if (*encoded_layout_version != *layout_version ||
                *header > std::numeric_limits<size_t>::max() ||
                *presence_offset > std::numeric_limits<size_t>::max() ||
                *presence_words > std::numeric_limits<size_t>::max() ||
                *fixed_offset > std::numeric_limits<size_t>::max() ||
                *fixed_size > std::numeric_limits<size_t>::max() ||
                (*unknown != kNoOffset &&
                 *unknown > std::numeric_limits<size_t>::max()) ||
                *object_size > std::numeric_limits<size_t>::max() ||
                *alignment > std::numeric_limits<size_t>::max()) {
                return Invalid("descriptor layout dimensions exceed host limits");
            }
            DecodedLayoutArtifact layout{
                .layout_version = *encoded_layout_version,
                .header_size = static_cast<size_t>(*header),
                .presence_bitmap_offset = static_cast<size_t>(*presence_offset),
                .presence_bitmap_words = static_cast<size_t>(*presence_words),
                .fixed_area_offset = static_cast<size_t>(*fixed_offset),
                .fixed_area_size = static_cast<size_t>(*fixed_size),
                .unknown_fields_offset = *unknown == kNoOffset
                    ? std::nullopt
                    : std::optional<size_t>(static_cast<size_t>(*unknown)),
                .object_size = static_cast<size_t>(*object_size),
                .object_alignment = static_cast<size_t>(*alignment),
                .max_child_bytes = *child_bytes,
                .max_dynamic_children = *children,
                .fields = std::move(field_layouts),
            };
            auto descriptor = std::make_shared<const SchemaDescriptor>(
                std::move(aggregate),
                SchemaIdentity(*short_id, *digest, *schema_version,
                               *layout_version),
                std::move(*canonical_bytes), std::move(dependencies));
            const Status layout_status =
                ValidateLayoutShape(*descriptor, layout);
            if (!layout_status.ok()) return layout_status;
            const auto existing = identities.find(*full_name);
            if (existing != identities.end() && existing->second != *digest) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "descriptor FQN maps to multiple digests");
            }
            const auto short_existing = short_ids.find(*short_id);
            if (short_existing != short_ids.end() && short_existing->second != *digest) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "descriptor short ID collision");
            }
            identities.insert_or_assign(*full_name, *digest);
            short_ids.insert_or_assign(*short_id, *digest);
            result.types.push_back(
                DecodedTypeArtifact{std::move(descriptor), std::move(layout)});
        }
        if (!reader.done()) return Invalid("descriptor artifact has trailing bytes");

        std::vector<std::shared_ptr<const SchemaDescriptor>> candidates(
            external_descriptors.begin(), external_descriptors.end());
        candidates.reserve(candidates.size() + result.types.size());
        for (const DecodedTypeArtifact& type : result.types) {
            candidates.push_back(type.descriptor);
        }
        for (const DecodedTypeArtifact& type : result.types) {
            std::vector<std::shared_ptr<const SchemaDescriptor>> exact_closure;
            exact_closure.reserve(type.descriptor->dependencies().size());
            for (const DependencyDescriptor& dependency :
                 type.descriptor->dependencies()) {
                const auto resolved = std::find_if(
                    candidates.begin(), candidates.end(),
                    [&](const auto& candidate) {
                        return candidate != nullptr &&
                               candidate->aggregate().full_name() ==
                                   dependency.full_name() &&
                               candidate->identity().canonical_digest() ==
                                   dependency.digest();
                    });
                if (resolved == candidates.end()) {
                    return Status::Error(
                        StatusCode::kSchemaMismatch,
                        "descriptor layout dependency closure is unavailable");
                }
                exact_closure.push_back(*resolved);
            }
            const Status canonical_layout = ValidateCanonicalLayout(
                *type.descriptor, type.layout, exact_closure);
            if (!canonical_layout.ok()) return canonical_layout;
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<DecodedDescriptorArtifact> DecodeAndValidate(
    std::string_view bytes) noexcept {
    return DecodeAndValidate(bytes, {});
}

}  // namespace mino::schema::codegen
