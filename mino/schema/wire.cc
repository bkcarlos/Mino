// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/wire.h"

#include <algorithm>
#include <bit>
#include <limits>
#include <map>
#include <new>
#include <string>
#include <utility>

#include "mino/common/status.h"
#include "mino/schema/descriptor_closure.h"

namespace mino::schema {
namespace {

constexpr uint32_t kMaxFieldId = 536870911u;

Status Corruption(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}

Status Mismatch(std::string_view message) {
    return Status::Error(StatusCode::kSchemaMismatch, message);
}

Status Resource(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

bool IsValidUtf8(std::span<const std::byte> bytes) noexcept {
    size_t i = 0;
    while (i < bytes.size()) {
        const uint8_t first = static_cast<uint8_t>(bytes[i]);
        if (first <= 0x7f) {
            ++i;
            continue;
        }
        size_t length = 0;
        uint32_t codepoint = 0;
        uint32_t minimum = 0;
        if ((first & 0xe0u) == 0xc0u) {
            length = 2;
            codepoint = first & 0x1fu;
            minimum = 0x80;
        } else if ((first & 0xf0u) == 0xe0u) {
            length = 3;
            codepoint = first & 0x0fu;
            minimum = 0x800;
        } else if ((first & 0xf8u) == 0xf0u) {
            length = 4;
            codepoint = first & 0x07u;
            minimum = 0x10000;
        } else {
            return false;
        }
        if (length > bytes.size() - i) return false;
        for (size_t j = 1; j < length; ++j) {
            const uint8_t next = static_cast<uint8_t>(bytes[i + j]);
            if ((next & 0xc0u) != 0x80u) return false;
            codepoint = (codepoint << 6) | (next & 0x3fu);
        }
        if (codepoint < minimum || codepoint > 0x10ffffu ||
            (codepoint >= 0xd800u && codepoint <= 0xdfffu)) {
            return false;
        }
        i += length;
    }
    return true;
}

Status Append(std::span<const std::byte> bytes, size_t limit,
              std::vector<std::byte>& output) {
    if (bytes.size() > limit || output.size() > limit - bytes.size()) {
        return Resource("canonical frame exceeds max_frame_bytes");
    }
    output.insert(output.end(), bytes.begin(), bytes.end());
    return Status::Ok();
}



Status AppendLeb128(uint64_t value, size_t limit,
                    std::vector<std::byte>& output) {
    std::byte bytes[10];
    size_t count = 0;
    do {
        uint8_t byte = static_cast<uint8_t>(value & 0x7fu);
        value >>= 7;
        if (value != 0) byte |= 0x80u;
        bytes[count++] = static_cast<std::byte>(byte);
    } while (value != 0);
    return Append(std::span<const std::byte>(bytes, count), limit, output);
}

Status AppendFixed32(uint32_t value, size_t limit,
                     std::vector<std::byte>& output) {
    std::byte bytes[4];
    for (size_t i = 0; i < 4; ++i) {
        bytes[i] = static_cast<std::byte>((value >> (8 * i)) & 0xffu);
    }
    return Append(bytes, limit, output);
}

Status AppendFixed64(uint64_t value, size_t limit,
                     std::vector<std::byte>& output) {
    std::byte bytes[8];
    for (size_t i = 0; i < 8; ++i) {
        bytes[i] = static_cast<std::byte>((value >> (8 * i)) & 0xffu);
    }
    return Append(bytes, limit, output);
}

class Reader {
public:
    Reader(std::span<const std::byte> bytes, const WireLimits& limits)
        : bytes_(bytes), limits_(limits) {}

    bool done() const noexcept { return offset_ == bytes_.size(); }
    size_t offset() const noexcept { return offset_; }
    std::span<const std::byte> bytes() const noexcept { return bytes_; }
    std::span<const std::byte> remaining() const noexcept {
        return bytes_.subspan(offset_);
    }
    void ConsumeAll() noexcept { offset_ = bytes_.size(); }

    Result<uint64_t> Varint() {
        return DecodeLeb128(bytes_, offset_);
    }

    Result<uint32_t> Fixed32() {
        if (bytes_.size() - offset_ < 4) {
            return Corruption("truncated I32 field");
        }
        uint32_t value = 0;
        for (size_t i = 0; i < 4; ++i) {
            value |= static_cast<uint32_t>(bytes_[offset_ + i]) << (8 * i);
        }
        offset_ += 4;
        return value;
    }

    Result<uint64_t> Fixed64() {
        if (bytes_.size() - offset_ < 8) {
            return Corruption("truncated I64 field");
        }
        uint64_t value = 0;
        for (size_t i = 0; i < 8; ++i) {
            value |= static_cast<uint64_t>(bytes_[offset_ + i]) << (8 * i);
        }
        offset_ += 8;
        return value;
    }

    Result<std::span<const std::byte>> LengthDelimited() {
        auto length = Varint();
        if (!length.ok()) return length.status();
        if (*length > limits_.max_length_bytes) {
            return Resource("length-delimited value exceeds max_length_bytes");
        }
        if (*length > bytes_.size() - offset_) {
            return Corruption("truncated length-delimited field");
        }
        const size_t size = static_cast<size_t>(*length);
        const auto result = bytes_.subspan(offset_, size);
        offset_ += size;
        return result;
    }

    Status Skip(WireType wire_type) {
        switch (wire_type) {
            case WireType::kVarint: {
                auto value = Varint();
                return value.ok() ? Status::Ok() : value.status();
            }
            case WireType::kI64: {
                auto value = Fixed64();
                return value.ok() ? Status::Ok() : value.status();
            }
            case WireType::kLengthDelimited: {
                auto value = LengthDelimited();
                return value.ok() ? Status::Ok() : value.status();
            }
            case WireType::kI32: {
                auto value = Fixed32();
                return value.ok() ? Status::Ok() : value.status();
            }
        }
        return Corruption("unknown wire type");
    }

private:
    std::span<const std::byte> bytes_;
    const WireLimits& limits_;
    size_t offset_ = 0;
};

Result<WireType> ParseWireType(uint64_t value) {
    switch (value) {
        case 0:
            return WireType::kVarint;
        case 1:
            return WireType::kI64;
        case 2:
            return WireType::kLengthDelimited;
        case 5:
            return WireType::kI32;
        default:
            return Corruption("field has an unsupported wire type");
    }
}

Result<WireType> ExpectedWireType(const TypeDescriptor& type) {
    if (type.kind() != TypeDescriptor::Kind::kScalar) {
        return WireType::kLengthDelimited;
    }
    if (!type.scalar().has_value()) {
        return Mismatch("scalar descriptor is incomplete");
    }
    switch (*type.scalar()) {
        case ScalarType::kInt32:
        case ScalarType::kInt64:
        case ScalarType::kUint32:
        case ScalarType::kUint64:
        case ScalarType::kBool:
            return WireType::kVarint;
        case ScalarType::kFixed64:
        case ScalarType::kDouble:
            return WireType::kI64;
        case ScalarType::kString:
        case ScalarType::kBytes:
            return WireType::kLengthDelimited;
        case ScalarType::kFixed32:
        case ScalarType::kFloat:
            return WireType::kI32;
    }
    return Mismatch("unsupported descriptor type");
}

class DescriptorResolver {
public:
    DescriptorResolver(
        const SchemaDescriptor& root,
        std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors) {
        descriptors_.emplace(std::string(root.aggregate().full_name()), &root);
        for (const auto& descriptor : descriptors) {
            if (descriptor != nullptr) {
                descriptors_.try_emplace(
                    std::string(descriptor->aggregate().full_name()),
                    descriptor.get());
            }
        }
    }

    Result<const SchemaDescriptor*> Find(std::string_view name) const {
        const auto it = descriptors_.find(name);
        if (it == descriptors_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "wire descriptor dependency is unavailable");
        }
        return it->second;
    }

private:
    std::map<std::string, const SchemaDescriptor*, std::less<>> descriptors_;
};

struct CodecContext {
    const DescriptorResolver& resolver;
    const WireLimits& limits;
    size_t decoded_elements = 0;
};



Status EncodeValue(const TypeDescriptor& type, const ConstraintSet& constraints,
                   const DynamicValue& value, size_t depth,
                   CodecContext& context, std::vector<std::byte>& output);

Status EncodeMessage(const SchemaDescriptor& descriptor,
                     const DynamicMessage& message, size_t depth,
                     CodecContext& context, std::vector<std::byte>& output);

Status EncodeLengthPayload(std::span<const std::byte> payload,
                           CodecContext& context,
                           std::vector<std::byte>& output) {
    if (payload.size() > context.limits.max_length_bytes) {
        return Resource("length-delimited value exceeds max_length_bytes");
    }
    Status status = AppendLeb128(payload.size(), context.limits.max_frame_bytes,
                                 output);
    if (!status.ok()) return status;
    return Append(payload, context.limits.max_frame_bytes, output);
}

Status EncodeVectorPayload(const TypeDescriptor& element_type,
                           const ConstraintSet& constraints,
                           const DynamicVector& vector, size_t depth,
                           CodecContext& context,
                           std::vector<std::byte>& output) {
    const size_t size = vector.values().size();
    if (size > context.limits.max_container_elements ||
        (constraints.max_capacity().has_value() &&
         size > *constraints.max_capacity())) {
        return Resource("vector element count exceeds limit");
    }
    Status status =
        AppendLeb128(size, context.limits.max_frame_bytes, output);
    if (!status.ok()) return status;
    for (const DynamicValue& element : vector.values()) {
        if (element_type.kind() == TypeDescriptor::Kind::kScalar &&
            element_type.scalar() != ScalarType::kString &&
            element_type.scalar() != ScalarType::kBytes) {
            status = EncodeValue(element_type, constraints, element, depth + 1,
                                 context, output);
        } else {
            std::vector<std::byte> payload;
            status = EncodeValue(element_type, constraints, element, depth + 1,
                                 context, payload);
            if (status.ok()) {
                status = EncodeLengthPayload(payload, context, output);
            }
        }
        if (!status.ok()) return status;
    }
    return Status::Ok();
}

Status EncodeValue(const TypeDescriptor& type, const ConstraintSet& constraints,
                   const DynamicValue& value, size_t depth,
                   CodecContext& context, std::vector<std::byte>& output) {
    if (depth > context.limits.max_depth) {
        return Resource("canonical nesting exceeds max_depth");
    }
    if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
        const MessageValue* nested = value.message();
        if (nested == nullptr || nested->value == nullptr) {
            return Mismatch("user-defined field requires a DynamicMessage");
        }
        auto descriptor = context.resolver.Find(type.name());
        if (!descriptor.ok()) return descriptor.status();
        return EncodeMessage(**descriptor, *nested->value, depth, context,
                             output);
    }
    if (type.kind() == TypeDescriptor::Kind::kVector) {
        const VectorValue* vector = value.vector();
        if (vector == nullptr || vector->value == nullptr ||
            type.element_type() == nullptr) {
            return Mismatch("vector field requires a DynamicVector");
        }
        return EncodeVectorPayload(*type.element_type(), constraints,
                                   *vector->value, depth, context, output);
    }
    if (!type.scalar().has_value()) {
        return Mismatch("scalar descriptor is incomplete");
    }
    switch (*type.scalar()) {
        case ScalarType::kInt32: {
            const auto* integer = value.signed_integer();
            if (integer == nullptr ||
                integer->value < std::numeric_limits<int32_t>::min() ||
                integer->value > std::numeric_limits<int32_t>::max()) {
                return Mismatch("int32 value has the wrong dynamic type/range");
            }
            return AppendLeb128(ZigZagEncode(integer->value),
                                context.limits.max_frame_bytes, output);
        }
        case ScalarType::kInt64: {
            const auto* integer = value.signed_integer();
            if (integer == nullptr) {
                return Mismatch("int64 value has the wrong dynamic type");
            }
            return AppendLeb128(ZigZagEncode(integer->value),
                                context.limits.max_frame_bytes, output);
        }
        case ScalarType::kUint32: {
            const auto* integer = value.unsigned_integer();
            if (integer == nullptr ||
                integer->value > std::numeric_limits<uint32_t>::max()) {
                return Mismatch("uint32 value has the wrong dynamic type/range");
            }
            return AppendLeb128(integer->value,
                                context.limits.max_frame_bytes, output);
        }
        case ScalarType::kUint64: {
            const auto* integer = value.unsigned_integer();
            if (integer == nullptr) {
                return Mismatch("uint64 value has the wrong dynamic type");
            }
            return AppendLeb128(integer->value,
                                context.limits.max_frame_bytes, output);
        }
        case ScalarType::kFixed32: {
            const auto* integer = value.unsigned_integer();
            if (integer == nullptr ||
                integer->value > std::numeric_limits<uint32_t>::max()) {
                return Mismatch("fixed32 value has the wrong dynamic type/range");
            }
            return AppendFixed32(static_cast<uint32_t>(integer->value),
                                 context.limits.max_frame_bytes, output);
        }
        case ScalarType::kFixed64: {
            const auto* integer = value.unsigned_integer();
            if (integer == nullptr) {
                return Mismatch("fixed64 value has the wrong dynamic type");
            }
            return AppendFixed64(integer->value,
                                 context.limits.max_frame_bytes, output);
        }
        case ScalarType::kFloat: {
            const auto* floating = value.float32();
            if (floating == nullptr) {
                return Mismatch("float value requires exact Float32 bits");
            }
            return AppendFixed32(floating->bits,
                                 context.limits.max_frame_bytes, output);
        }
        case ScalarType::kDouble: {
            const auto* floating = value.float64();
            if (floating == nullptr) {
                return Mismatch("double value requires exact Float64 bits");
            }
            return AppendFixed64(floating->bits,
                                 context.limits.max_frame_bytes, output);
        }
        case ScalarType::kBool: {
            const auto* boolean = value.boolean();
            if (boolean == nullptr) {
                return Mismatch("bool value has the wrong dynamic type");
            }
            return AppendLeb128(boolean->value ? 1 : 0,
                                context.limits.max_frame_bytes, output);
        }
        case ScalarType::kString: {
            const auto* string = value.string();
            if (string == nullptr) {
                return Mismatch("string value has the wrong dynamic type");
            }
            const auto bytes = std::as_bytes(std::span(string->value));
            if (!IsValidUtf8(bytes)) return Mismatch("string is not valid UTF-8");
            if (constraints.max_bytes().has_value() &&
                bytes.size() > *constraints.max_bytes()) {
                return Resource("string exceeds descriptor max_bytes");
            }
            return Append(bytes, context.limits.max_frame_bytes, output);
        }
        case ScalarType::kBytes: {
            const auto* bytes = value.bytes();
            if (bytes == nullptr) {
                return Mismatch("bytes value has the wrong dynamic type");
            }
            if (constraints.max_bytes().has_value() &&
                bytes->value.size() > *constraints.max_bytes()) {
                return Resource("bytes exceeds descriptor max_bytes");
            }
            return Append(bytes->value, context.limits.max_frame_bytes, output);
        }
    }
    return Mismatch("unsupported scalar type");
}

Status ValidateUnknownCanonical(const UnknownField& field,
                                const WireLimits& limits) {
    Reader reader(field.canonical_bytes(), limits);
    auto tag = reader.Varint();
    if (!tag.ok()) return tag.status();
    const uint64_t field_id = *tag >> 3;
    if (field_id == 0 || field_id > kMaxFieldId ||
        field_id != field.field_id()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "unknown canonical bytes have the wrong field ID");
    }
    auto wire_type = ParseWireType(*tag & 7u);
    if (!wire_type.ok()) return wire_type.status();
    Status status = reader.Skip(*wire_type);
    if (!status.ok()) return status;
    if (!reader.done()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "unknown field bytes contain multiple fields");
    }
    return Status::Ok();
}

struct EncodedField {
    uint32_t id = 0;
    size_t order = 0;
    std::vector<std::byte> bytes;
};

Status EncodeMessage(const SchemaDescriptor& descriptor,
                     const DynamicMessage& message, size_t depth,
                     CodecContext& context, std::vector<std::byte>& output) {
    if (depth > context.limits.max_depth) {
        return Resource("canonical nesting exceeds max_depth");
    }
    for (const DynamicField& field : message.fields()) {
        if (descriptor.aggregate().FindField(field.id()) == nullptr) {
            return Mismatch("DynamicMessage contains a field absent from descriptor");
        }
    }
    if (message.unknown_fields().fields().size() >
            context.limits.unknown_fields.max_fields ||
        message.unknown_fields().byte_size() >
            context.limits.unknown_fields.max_bytes) {
        return Resource("unknown field set exceeds wire limits");
    }

    std::vector<EncodedField> encoded;
    encoded.reserve(descriptor.aggregate().fields().size() +
                    message.unknown_fields().fields().size());
    size_t order = 0;
    for (const FieldDescriptor& field : descriptor.aggregate().fields()) {
        const DynamicValue* value = message.FindField(field.id());
        if (value == nullptr) {
            if (field.cardinality() == FieldCardinality::kOptional) continue;
            return Mismatch("required field is missing from DynamicMessage");
        }
        auto wire_type = ExpectedWireType(field.type());
        if (!wire_type.ok()) return wire_type.status();
        EncodedField item;
        item.id = field.id();
        item.order = order++;
        Status status = AppendLeb128(
            (static_cast<uint64_t>(field.id()) << 3) |
                static_cast<uint8_t>(*wire_type),
            context.limits.max_frame_bytes, item.bytes);
        if (!status.ok()) return status;
        if (*wire_type == WireType::kLengthDelimited) {
            std::vector<std::byte> payload;
            status = EncodeValue(field.type(), field.constraints(), *value,
                                 depth + 1, context, payload);
            if (status.ok()) {
                status = EncodeLengthPayload(payload, context, item.bytes);
            }
        } else {
            status = EncodeValue(field.type(), field.constraints(), *value,
                                 depth + 1, context, item.bytes);
        }
        if (!status.ok()) return status;
        encoded.push_back(std::move(item));
    }
    for (const UnknownField& field : message.unknown_fields().fields()) {
        if (descriptor.aggregate().FindField(field.field_id()) != nullptr) {
            return Mismatch("unknown field collides with a known descriptor field");
        }
        Status status = ValidateUnknownCanonical(field, context.limits);
        if (!status.ok()) return status;
        EncodedField item;
        item.id = field.field_id();
        item.order = order++;
        item.bytes.assign(field.canonical_bytes().begin(),
                          field.canonical_bytes().end());
        encoded.push_back(std::move(item));
    }
    std::stable_sort(encoded.begin(), encoded.end(),
                     [](const EncodedField& lhs, const EncodedField& rhs) {
                         return lhs.id < rhs.id;
                     });
    for (const EncodedField& field : encoded) {
        Status status = Append(field.bytes, context.limits.max_frame_bytes,
                               output);
        if (!status.ok()) return status;
    }
    return Status::Ok();
}

Result<DynamicValue> DecodeValue(const TypeDescriptor& type,
                                 const ConstraintSet& constraints,
                                 WireType wire_type, Reader& reader,
                                 size_t depth, CodecContext& context);

Result<DynamicMessage> DecodeMessage(const SchemaDescriptor& descriptor,
                                     std::span<const std::byte> bytes,
                                     size_t depth, CodecContext& context);

Result<DynamicValue> DecodeElement(const TypeDescriptor& type,
                                   const ConstraintSet& constraints,
                                   Reader& reader, size_t depth,
                                   CodecContext& context) {
    auto expected = ExpectedWireType(type);
    if (!expected.ok()) return expected.status();
    if (*expected == WireType::kLengthDelimited) {
        auto payload = reader.LengthDelimited();
        if (!payload.ok()) return payload.status();
        Reader nested(*payload, context.limits);
        auto value = DecodeValue(type, constraints, *expected, nested, depth,
                                 context);
        if (!value.ok()) return value.status();
        if (!nested.done()) return Corruption("vector element has trailing bytes");
        return value;
    }
    return DecodeValue(type, constraints, *expected, reader, depth, context);
}

Result<DynamicValue> DecodeValue(const TypeDescriptor& type,
                                 const ConstraintSet& constraints,
                                 WireType wire_type, Reader& reader,
                                 size_t depth, CodecContext& context) {
    if (depth > context.limits.max_depth) {
        return Resource("canonical nesting exceeds max_depth");
    }
    if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
        if (wire_type != WireType::kLengthDelimited) {
            return Mismatch("nested message has the wrong wire type");
        }
        auto descriptor = context.resolver.Find(type.name());
        if (!descriptor.ok()) return descriptor.status();
        auto nested = DecodeMessage(**descriptor, reader.remaining(), depth,
                                    context);
        if (!nested.ok()) return nested.status();
        reader.ConsumeAll();
        auto pointer = std::make_shared<DynamicMessage>(std::move(*nested));
        return DynamicValue::Message(std::move(pointer));
    }
    if (type.kind() == TypeDescriptor::Kind::kVector) {
        if (wire_type != WireType::kLengthDelimited ||
            type.element_type() == nullptr) {
            return Mismatch("vector has the wrong wire type/descriptor");
        }
        auto count = reader.Varint();
        if (!count.ok()) return count.status();
        if (*count > context.limits.max_container_elements ||
            (constraints.max_capacity().has_value() &&
             *count > *constraints.max_capacity())) {
            return Resource("vector element count exceeds limit");
        }
        if (*count > std::numeric_limits<size_t>::max() -
                         context.decoded_elements) {
            return Resource("decoded element count overflows");
        }
        context.decoded_elements += static_cast<size_t>(*count);
        if (context.decoded_elements >
            context.limits.max_container_elements) {
            return Resource("aggregate decoded element count exceeds limit");
        }
        auto vector = std::make_shared<DynamicVector>();
        for (uint64_t i = 0; i < *count; ++i) {
            auto element = DecodeElement(*type.element_type(), constraints,
                                         reader, depth + 1, context);
            if (!element.ok()) return element.status();
            Status status = vector->Add(std::move(*element));
            if (!status.ok()) return status;
        }
        if (!reader.done()) return Corruption("vector payload has trailing bytes");
        return DynamicValue::Vector(std::move(vector));
    }
    if (!type.scalar().has_value()) {
        return Mismatch("scalar descriptor is incomplete");
    }
    switch (*type.scalar()) {
        case ScalarType::kInt32: {
            auto encoded = reader.Varint();
            if (!encoded.ok()) return encoded.status();
            const int64_t value = ZigZagDecode(*encoded);
            if (value < std::numeric_limits<int32_t>::min() ||
                value > std::numeric_limits<int32_t>::max()) {
                return Corruption("decoded int32 is out of range");
            }
            return DynamicValue::Signed(value);
        }
        case ScalarType::kInt64: {
            auto encoded = reader.Varint();
            if (!encoded.ok()) return encoded.status();
            return DynamicValue::Signed(ZigZagDecode(*encoded));
        }
        case ScalarType::kUint32: {
            auto encoded = reader.Varint();
            if (!encoded.ok()) return encoded.status();
            if (*encoded > std::numeric_limits<uint32_t>::max()) {
                return Corruption("decoded uint32 is out of range");
            }
            return DynamicValue::Unsigned(*encoded);
        }
        case ScalarType::kUint64: {
            auto encoded = reader.Varint();
            if (!encoded.ok()) return encoded.status();
            return DynamicValue::Unsigned(*encoded);
        }
        case ScalarType::kFixed32: {
            auto encoded = reader.Fixed32();
            if (!encoded.ok()) return encoded.status();
            return DynamicValue::Unsigned(*encoded);
        }
        case ScalarType::kFixed64: {
            auto encoded = reader.Fixed64();
            if (!encoded.ok()) return encoded.status();
            return DynamicValue::Unsigned(*encoded);
        }
        case ScalarType::kFloat: {
            auto bits = reader.Fixed32();
            if (!bits.ok()) return bits.status();
            return DynamicValue::Float32Bits(*bits);
        }
        case ScalarType::kDouble: {
            auto bits = reader.Fixed64();
            if (!bits.ok()) return bits.status();
            return DynamicValue::Float64Bits(*bits);
        }
        case ScalarType::kBool: {
            auto encoded = reader.Varint();
            if (!encoded.ok()) return encoded.status();
            if (*encoded > 1) return Corruption("bool must be encoded as 0 or 1");
            return DynamicValue::Boolean(*encoded != 0);
        }
        case ScalarType::kString: {
            const auto bytes = reader.remaining();
            if (!IsValidUtf8(bytes)) return Corruption("string is not valid UTF-8");
            if (constraints.max_bytes().has_value() &&
                bytes.size() > *constraints.max_bytes()) {
                return Resource("string exceeds descriptor max_bytes");
            }
            reader.ConsumeAll();
            return DynamicValue::String(std::string_view(
                reinterpret_cast<const char*>(bytes.data()), bytes.size()));
        }
        case ScalarType::kBytes: {
            const auto bytes = reader.remaining();
            if (constraints.max_bytes().has_value() &&
                bytes.size() > *constraints.max_bytes()) {
                return Resource("bytes exceeds descriptor max_bytes");
            }
            reader.ConsumeAll();
            return DynamicValue::Bytes(bytes);
        }
    }
    return Mismatch("unsupported scalar type");
}

Result<DynamicMessage> DecodeMessage(const SchemaDescriptor& descriptor,
                                     std::span<const std::byte> bytes,
                                     size_t depth, CodecContext& context) {
    if (depth > context.limits.max_depth) {
        return Resource("canonical nesting exceeds max_depth");
    }
    if (bytes.size() > context.limits.max_frame_bytes) {
        return Resource("canonical frame exceeds max_frame_bytes");
    }
    DynamicMessage message(context.limits.unknown_fields);
    Reader reader(bytes, context.limits);
    uint32_t previous_id = 0;
    std::vector<uint32_t> seen;
    while (!reader.done()) {
        const size_t field_start = reader.offset();
        auto tag = reader.Varint();
        if (!tag.ok()) return tag.status();
        const uint64_t id64 = *tag >> 3;
        if (id64 == 0 || id64 > kMaxFieldId) {
            return Corruption("field ID is outside canonical range");
        }
        const uint32_t id = static_cast<uint32_t>(id64);
        if (id < previous_id) {
            return Corruption("canonical fields are not sorted by field ID");
        }
        previous_id = id;
        auto wire_type = ParseWireType(*tag & 7u);
        if (!wire_type.ok()) return wire_type.status();
        const FieldDescriptor* field = descriptor.aggregate().FindField(id);
        if (field == nullptr) {
            Status status = reader.Skip(*wire_type);
            if (!status.ok()) return status;
            const auto raw = bytes.subspan(field_start,
                                            reader.offset() - field_start);
            status = message.mutable_unknown_fields().Add(id, raw);
            if (!status.ok()) return status;
            continue;
        }
        if (std::binary_search(seen.begin(), seen.end(), id)) {
            return Corruption("known field is encoded more than once");
        }
        seen.insert(std::lower_bound(seen.begin(), seen.end(), id), id);
        auto expected = ExpectedWireType(field->type());
        if (!expected.ok()) return expected.status();
        if (*expected != *wire_type) {
            return Mismatch("wire type does not match field descriptor");
        }

        Result<DynamicValue> value = Corruption("uninitialized field decode");
        if (*wire_type == WireType::kLengthDelimited) {
            auto payload = reader.LengthDelimited();
            if (!payload.ok()) return payload.status();
            Reader payload_reader(*payload, context.limits);
            value = DecodeValue(field->type(), field->constraints(), *wire_type,
                                payload_reader, depth + 1, context);
            if (value.ok() && !payload_reader.done()) {
                return Corruption("length-delimited field has trailing bytes");
            }
        } else {
            value = DecodeValue(field->type(), field->constraints(), *wire_type,
                                reader, depth + 1, context);
        }
        if (!value.ok()) return value.status();
        Status status = message.SetField(id, std::move(*value));
        if (!status.ok()) return status;
    }
    for (const FieldDescriptor& field : descriptor.aggregate().fields()) {
        if (field.cardinality() != FieldCardinality::kOptional &&
            message.FindField(field.id()) == nullptr) {
            return Mismatch("required field is absent from canonical frame");
        }
    }
    return message;
}

}  // namespace

uint64_t ZigZagEncode(int64_t value) noexcept {
    return (static_cast<uint64_t>(value) << 1) ^
           static_cast<uint64_t>(-(value < 0));
}

int64_t ZigZagDecode(uint64_t value) noexcept {
    const uint64_t bits = (value >> 1) ^ (0u - (value & 1u));
    return std::bit_cast<int64_t>(bits);
}

Status EncodeLeb128(uint64_t value,
                    std::vector<std::byte>& output) noexcept {
    try {
        do {
            uint8_t byte = static_cast<uint8_t>(value & 0x7fu);
            value >>= 7;
            if (value != 0) byte |= 0x80u;
            output.push_back(static_cast<std::byte>(byte));
        } while (value != 0);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<uint64_t> DecodeLeb128(std::span<const std::byte> input,
                              size_t& offset) noexcept {
    try {
        uint64_t value = 0;
        for (size_t i = 0; i < 10; ++i) {
            if (offset >= input.size()) {
                return Corruption("truncated LEB128");
            }
            const uint8_t byte = static_cast<uint8_t>(input[offset++]);
            if (i == 9 && (byte & 0xfeu) != 0) {
                return Corruption("LEB128 exceeds 64 bits");
            }
            value |= static_cast<uint64_t>(byte & 0x7fu) << (7 * i);
            if ((byte & 0x80u) == 0) {
                if (i != 0 && byte == 0) {
                    return Corruption("LEB128 is not minimally encoded");
                }
                return value;
            }
        }
        return Corruption("LEB128 exceeds ten bytes");
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<std::vector<std::byte>> CanonicalWireCodec::Encode(
    const SchemaDescriptor& descriptor, const DynamicMessage& message,
    std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors,
    const WireLimits& limits) noexcept {
    try {
        if (limits.max_frame_bytes == 0 || limits.max_depth == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "wire limits must be non-zero");
        }
        MINO_RETURN_IF_ERROR(ValidateDescriptorClosure(descriptor, descriptors));
        DescriptorResolver resolver(descriptor, descriptors);
        CodecContext context{resolver, limits, 0};
        std::vector<std::byte> output;
        Status status = EncodeMessage(descriptor, message, 0, context, output);
        if (!status.ok()) return status;
        return output;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<DynamicMessage> CanonicalWireCodec::Decode(
    const SchemaDescriptor& descriptor, std::span<const std::byte> bytes,
    std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors,
    const WireLimits& limits) noexcept {
    try {
        if (limits.max_frame_bytes == 0 || limits.max_depth == 0) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "wire limits must be non-zero");
        }
        MINO_RETURN_IF_ERROR(ValidateDescriptorClosure(descriptor, descriptors));
        DescriptorResolver resolver(descriptor, descriptors);
        CodecContext context{resolver, limits, 0};
        return DecodeMessage(descriptor, bytes, 0, context);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema
