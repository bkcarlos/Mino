// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#ifndef MINO_SCHEMA_DYNAMIC_VALUE_H_
#define MINO_SCHEMA_DYNAMIC_VALUE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "mino/common/result.h"
#include "mino/common/status.h"
#include "mino/schema/unknown_field_set.h"

namespace mino::schema {

class DynamicMessage;
class DynamicVector;

struct SignedIntegerValue { int64_t value = 0; };
struct UnsignedIntegerValue { uint64_t value = 0; };
struct Float32Value { uint32_t bits = 0; };
struct Float64Value { uint64_t bits = 0; };
struct BooleanValue { bool value = false; };
struct StringValue { std::string value; };
struct BytesValue { std::vector<std::byte> value; };
// Non-owning bytes for EncodeInto. The span must remain valid for the
// duration of the encode call that reads this DynamicValue.
struct BytesViewValue { std::span<const std::byte> value; };
struct MessageValue { std::shared_ptr<DynamicMessage> value; };
struct VectorValue { std::shared_ptr<DynamicVector> value; };

class DynamicValue {
public:
    enum class Kind {
        kSignedInteger,
        kUnsignedInteger,
        kFloat32,
        kFloat64,
        kBoolean,
        kString,
        kBytes,
        kMessage,
        kVector,
        kBytesView,
    };

    explicit DynamicValue(SignedIntegerValue value) noexcept : value_(value) {}
    explicit DynamicValue(UnsignedIntegerValue value) noexcept : value_(value) {}
    explicit DynamicValue(Float32Value value) noexcept : value_(value) {}
    explicit DynamicValue(Float64Value value) noexcept : value_(value) {}
    explicit DynamicValue(BooleanValue value) noexcept : value_(value) {}
    explicit DynamicValue(StringValue value) : value_(std::move(value)) {}
    explicit DynamicValue(BytesValue value) : value_(std::move(value)) {}
    explicit DynamicValue(BytesViewValue value) noexcept : value_(value) {}
    explicit DynamicValue(MessageValue value) noexcept : value_(std::move(value)) {}
    explicit DynamicValue(VectorValue value) noexcept : value_(std::move(value)) {}

    static DynamicValue Signed(int64_t value) noexcept;
    static DynamicValue Unsigned(uint64_t value) noexcept;
    static DynamicValue Float32Bits(uint32_t bits) noexcept;
    static DynamicValue Float64Bits(uint64_t bits) noexcept;
    static DynamicValue Boolean(bool value) noexcept;
    static Result<DynamicValue> String(std::string_view value) noexcept;
    static Result<DynamicValue> Bytes(std::span<const std::byte> value) noexcept;
    static DynamicValue BytesView(std::span<const std::byte> value) noexcept;
    static Result<DynamicValue> Message(
        std::shared_ptr<DynamicMessage> value) noexcept;
    static Result<DynamicValue> Vector(
        std::shared_ptr<DynamicVector> value) noexcept;

    Kind kind() const noexcept;
    const SignedIntegerValue* signed_integer() const noexcept;
    const UnsignedIntegerValue* unsigned_integer() const noexcept;
    const Float32Value* float32() const noexcept;
    const Float64Value* float64() const noexcept;
    const BooleanValue* boolean() const noexcept;
    const StringValue* string() const noexcept;
    const BytesValue* bytes() const noexcept;
    const BytesViewValue* bytes_view() const noexcept;
    const MessageValue* message() const noexcept;
    const VectorValue* vector() const noexcept;

private:
    using Storage = std::variant<SignedIntegerValue, UnsignedIntegerValue,
                                 Float32Value, Float64Value, BooleanValue,
                                 StringValue, BytesValue, MessageValue,
                                 VectorValue, BytesViewValue>;
    Storage value_;
};

class DynamicVector {
public:
    Status Add(DynamicValue value) noexcept;
    std::span<const DynamicValue> values() const noexcept { return values_; }

private:
    std::vector<DynamicValue> values_;
};

class DynamicField {
public:
    DynamicField(uint32_t id, DynamicValue value) noexcept
        : id_(id), value_(std::move(value)) {}

    uint32_t id() const noexcept { return id_; }
    const DynamicValue& value() const noexcept { return value_; }

private:
    uint32_t id_ = 0;
    DynamicValue value_;
};

class DynamicMessage {
public:
    explicit DynamicMessage(UnknownFieldLimits limits = {}) noexcept
        : unknown_fields_(limits) {}

    Status ReserveFields(size_t count) noexcept;
    void Clear() noexcept;

    Status SetField(uint32_t field_id, DynamicValue value) noexcept;
    const DynamicValue* FindField(uint32_t field_id) const noexcept;
    std::span<const DynamicField> fields() const noexcept { return fields_; }

    const UnknownFieldSet& unknown_fields() const noexcept {
        return unknown_fields_;
    }
    UnknownFieldSet& mutable_unknown_fields() noexcept { return unknown_fields_; }

private:
    std::vector<DynamicField> fields_;
    UnknownFieldSet unknown_fields_;
};

}  // namespace mino::schema

#endif  // MINO_SCHEMA_DYNAMIC_VALUE_H_
