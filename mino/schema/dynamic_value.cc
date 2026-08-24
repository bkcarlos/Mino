// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/dynamic_value.h"

#include <algorithm>
#include <new>
#include <utility>

namespace mino::schema {

DynamicValue DynamicValue::Signed(int64_t value) noexcept {
    return DynamicValue(SignedIntegerValue{value});
}

DynamicValue DynamicValue::Unsigned(uint64_t value) noexcept {
    return DynamicValue(UnsignedIntegerValue{value});
}

DynamicValue DynamicValue::Float32Bits(uint32_t bits) noexcept {
    return DynamicValue(Float32Value{bits});
}

DynamicValue DynamicValue::Float64Bits(uint64_t bits) noexcept {
    return DynamicValue(Float64Value{bits});
}

DynamicValue DynamicValue::Boolean(bool value) noexcept {
    return DynamicValue(BooleanValue{value});
}

Result<DynamicValue> DynamicValue::String(std::string_view value) noexcept {
    try {
        return DynamicValue(StringValue{std::string(value)});
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<DynamicValue> DynamicValue::Bytes(
    std::span<const std::byte> value) noexcept {
    try {
        return DynamicValue(
            BytesValue{std::vector<std::byte>(value.begin(), value.end())});
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<DynamicValue> DynamicValue::Message(
    std::shared_ptr<DynamicMessage> value) noexcept {
    try {
        if (value == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "dynamic message is null");
        }
        return DynamicValue(MessageValue{std::move(value)});
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<DynamicValue> DynamicValue::Vector(
    std::shared_ptr<DynamicVector> value) noexcept {
    try {
        if (value == nullptr) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "dynamic vector is null");
        }
        return DynamicValue(VectorValue{std::move(value)});
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

DynamicValue::Kind DynamicValue::kind() const noexcept {
    return static_cast<Kind>(value_.index());
}

const SignedIntegerValue* DynamicValue::signed_integer() const noexcept {
    return std::get_if<SignedIntegerValue>(&value_);
}
const UnsignedIntegerValue* DynamicValue::unsigned_integer() const noexcept {
    return std::get_if<UnsignedIntegerValue>(&value_);
}
const Float32Value* DynamicValue::float32() const noexcept {
    return std::get_if<Float32Value>(&value_);
}
const Float64Value* DynamicValue::float64() const noexcept {
    return std::get_if<Float64Value>(&value_);
}
const BooleanValue* DynamicValue::boolean() const noexcept {
    return std::get_if<BooleanValue>(&value_);
}
const StringValue* DynamicValue::string() const noexcept {
    return std::get_if<StringValue>(&value_);
}
const BytesValue* DynamicValue::bytes() const noexcept {
    return std::get_if<BytesValue>(&value_);
}
const MessageValue* DynamicValue::message() const noexcept {
    return std::get_if<MessageValue>(&value_);
}
const VectorValue* DynamicValue::vector() const noexcept {
    return std::get_if<VectorValue>(&value_);
}

Status DynamicVector::Add(DynamicValue value) noexcept {
    try {
        values_.push_back(std::move(value));
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status DynamicMessage::ReserveFields(size_t count) noexcept {
    try {
        fields_.reserve(count);
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

void DynamicMessage::Clear() noexcept {
    fields_.clear();
    unknown_fields_.Clear();
}

Status DynamicMessage::SetField(uint32_t field_id, DynamicValue value) noexcept {
    try {
        if (field_id == 0 || field_id > 536870911u) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "field ID is outside canonical range");
        }
        auto it = std::lower_bound(
            fields_.begin(), fields_.end(), field_id,
            [](const DynamicField& field, uint32_t id) {
                return field.id() < id;
            });
        if (it != fields_.end() && it->id() == field_id) {
            *it = DynamicField(field_id, std::move(value));
        } else {
            fields_.insert(it, DynamicField(field_id, std::move(value)));
        }
        return Status::Ok();
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

const DynamicValue* DynamicMessage::FindField(uint32_t field_id) const noexcept {
    const auto it = std::lower_bound(
        fields_.begin(), fields_.end(), field_id,
        [](const DynamicField& field, uint32_t id) {
            return field.id() < id;
        });
    return it != fields_.end() && it->id() == field_id ? &it->value() : nullptr;
}

}  // namespace mino::schema
