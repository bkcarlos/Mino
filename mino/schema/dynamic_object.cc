// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/dynamic_object.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <map>
#include <new>
#include <string>
#include <utility>
#include <vector>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/descriptor_closure.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino::schema {
namespace {

Status Mismatch(std::string_view message) {
    return Status::Error(StatusCode::kSchemaMismatch, message);
}
Status Corrupt(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
}
Status Resource(std::string_view message) {
    return Status::Error(StatusCode::kResourceExhausted, message);
}

void Write32(std::byte* data, uint32_t value) noexcept {
    for (size_t i = 0; i < 4; ++i) {
        data[i] = static_cast<std::byte>((value >> (8 * i)) & 0xffu);
    }
}
void Write64(std::byte* data, uint64_t value) noexcept {
    for (size_t i = 0; i < 8; ++i) {
        data[i] = static_cast<std::byte>((value >> (8 * i)) & 0xffu);
    }
}
uint32_t Read32(const std::byte* data) noexcept {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) {
        value |= static_cast<uint32_t>(static_cast<uint8_t>(data[i])) << (8 * i);
    }
    return value;
}
uint64_t Read64(const std::byte* data) noexcept {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) {
        value |= static_cast<uint64_t>(static_cast<uint8_t>(data[i])) << (8 * i);
    }
    return value;
}
void WriteHandle(std::byte* data, ShmHandle handle) noexcept {
    Write64(data, handle.offset);
    Write32(data + 8, handle.generation);
    Write32(data + 12, handle.region_id);
}
ShmHandle ReadHandle(const std::byte* data) noexcept {
    return ShmHandle{.offset = Read64(data),
                     .generation = Read32(data + 8),
                     .region_id = Read32(data + 12)};
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

bool CheckedAdd(size_t lhs, size_t rhs, size_t& result) noexcept {
    if (lhs > std::numeric_limits<size_t>::max() - rhs) return false;
    result = lhs + rhs;
    return true;
}

bool CheckedMultiply(size_t lhs, size_t rhs, size_t& result) noexcept {
    if (lhs != 0 && rhs > std::numeric_limits<size_t>::max() / lhs) return false;
    result = lhs * rhs;
    return true;
}

void InitializeObject(const SchemaDescriptor& descriptor,
                      const LayoutPlan& layout, std::byte* data) noexcept {
    std::memset(data, 0, layout.object_size());
    Write32(data + ObjectHeaderLayout::kLayoutVersionOffset,
            layout.layout_version());
    Write32(data + ObjectHeaderLayout::kHeaderSizeOffset,
            static_cast<uint32_t>(layout.header_size()));
    Write64(data + ObjectHeaderLayout::kSchemaShortIdOffset,
            descriptor.identity().short_id());
    Write64(data + ObjectHeaderLayout::kObjectSizeOffset,
            layout.fixed_area_size());
    Write32(data + ObjectHeaderLayout::kFieldCountOffset,
            static_cast<uint32_t>(descriptor.aggregate().fields().size()));
    Write32(data + ObjectHeaderLayout::kPresenceBitmapWordsOffset,
            static_cast<uint32_t>(layout.presence_bitmap_words()));
}

struct ValueShape {
    size_t size = 0;
    size_t alignment = 1;
    FieldStorageKind storage = FieldStorageKind::kScalar;
};

}  // namespace

struct DynamicBuilder::Impl {
    explicit Impl(LayoutPlan planned_layout)
        : layout(std::move(planned_layout)) {}
    ~Impl() noexcept {
        if (transaction_active && journal != nullptr) {
            (void)journal->Abort(transaction);
        }
    }

    struct TrackedAllocation {
        ShmHandle handle;
        bool active = true;
    };

    DynamicSchemaHandle descriptor_owner;
    const SchemaDescriptor* descriptor = nullptr;
    LayoutPlan layout;
    CentralSlabAllocator* allocator = nullptr;
    AllocationJournal* journal = nullptr;
    TypeId type_id{};
    DynamicObjectOptions options;
    std::vector<std::shared_ptr<const SchemaDescriptor>> descriptor_owners;
    std::map<std::string, const SchemaDescriptor*, std::less<>> descriptors;
    std::map<std::string, LayoutPlan, std::less<>> layouts;
    ShmHandle root{};
    std::byte* root_data = nullptr;
    AllocationTransaction transaction;
    bool transaction_active = false;
    std::vector<TrackedAllocation> allocations;
    std::vector<std::vector<ShmHandle>> field_allocations;
    std::vector<ShmHandle> unknown_allocations;
    std::vector<bool> fields_set;

    Result<const SchemaDescriptor*> FindDescriptor(std::string_view name) const {
        const auto it = descriptors.find(name);
        if (it == descriptors.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "dynamic descriptor dependency is unavailable");
        }
        return it->second;
    }

    Result<const LayoutPlan*> FindPlan(const SchemaDescriptor& target) {
        const std::string name(target.aggregate().full_name());
        const auto found = layouts.find(name);
        if (found != layouts.end()) return &found->second;
        std::vector<std::shared_ptr<const SchemaDescriptor>> closure;
        closure.reserve(target.dependencies().size());
        for (const DependencyDescriptor& dependency : target.dependencies()) {
            const auto owner = std::find_if(
                descriptor_owners.begin(), descriptor_owners.end(),
                [&](const auto& candidate) {
                    return candidate != nullptr &&
                           candidate->aggregate().full_name() ==
                               dependency.full_name() &&
                           candidate->identity().canonical_digest() ==
                               dependency.digest();
                });
            if (owner != descriptor_owners.end()) closure.push_back(*owner);
        }
        auto planned = LayoutPlanner::Plan(target, closure);
        if (!planned.ok()) return planned.status();
        auto [it, inserted] = layouts.emplace(name, std::move(*planned));
        (void)inserted;
        return &it->second;
    }

    Result<ValueShape> Shape(const TypeDescriptor& type,
                             const ConstraintSet& constraints) {
        (void)constraints;
        if (type.kind() == TypeDescriptor::Kind::kScalar) {
            if (!type.scalar().has_value()) return Mismatch("incomplete scalar type");
            switch (*type.scalar()) {
                case ScalarType::kBool:
                    return ValueShape{1, 1, FieldStorageKind::kScalar};
                case ScalarType::kInt32:
                case ScalarType::kUint32:
                case ScalarType::kFixed32:
                case ScalarType::kFloat:
                    return ValueShape{4, 4, FieldStorageKind::kScalar};
                case ScalarType::kInt64:
                case ScalarType::kUint64:
                case ScalarType::kFixed64:
                case ScalarType::kDouble:
                    return ValueShape{8, 8, FieldStorageKind::kScalar};
                case ScalarType::kString:
                case ScalarType::kBytes:
                    return ValueShape{VariableMetadataLayout::kSize,
                                      VariableMetadataLayout::kAlignment,
                                      FieldStorageKind::kVariable};
            }
        }
        if (type.kind() == TypeDescriptor::Kind::kVector) {
            return ValueShape{VariableMetadataLayout::kSize,
                              VariableMetadataLayout::kAlignment,
                              FieldStorageKind::kVariable};
        }
        MINO_ASSIGN_OR_RETURN(const SchemaDescriptor* nested,
                              FindDescriptor(type.name()));
        MINO_ASSIGN_OR_RETURN(const LayoutPlan* nested_layout, FindPlan(*nested));
        if (nested->aggregate().kind() == AggregateKind::kStruct) {
            return ValueShape{nested_layout->object_size(),
                              nested_layout->object_alignment(),
                              FieldStorageKind::kInlineStruct};
        }
        return ValueShape{VariableMetadataLayout::kSize,
                          VariableMetadataLayout::kAlignment,
                          FieldStorageKind::kVariable};
    }

    Result<MutableBuildView> AllocateChild(
        size_t bytes, size_t alignment,
        const SchemaDescriptor& owning_descriptor) {
        size_t active_allocations = 0;
        for (const TrackedAllocation& allocation : allocations) {
            if (allocation.active) ++active_allocations;
        }
        if (active_allocations >= options.graph_limits.max_allocations) {
            return Resource("dynamic graph allocation limit exceeded");
        }
        if (bytes == 0 || bytes > std::numeric_limits<uint32_t>::max() ||
            alignment > std::numeric_limits<uint32_t>::max()) {
            return Resource("dynamic child allocation size is invalid");
        }
        AllocationRequest request;
        request.object_size = static_cast<uint32_t>(bytes);
        request.type_id = type_id;
        request.schema = mino::SchemaIdentity{
            .short_id = owning_descriptor.identity().short_id(),
            .layout_version = owning_descriptor.identity().layout_version()};
        request.alignment = static_cast<uint32_t>(alignment);
        MINO_ASSIGN_OR_RETURN(
            ShmHandle handle, journal->AllocateChild(transaction, request));
        auto build = allocator->BeginBuild(handle);
        if (!build.ok()) {
            (void)journal->ReleaseChild(transaction, handle);
            return build.status();
        }
        try {
            allocations.push_back(TrackedAllocation{handle, true});
        } catch (const std::bad_alloc&) {
            const Status released = journal->ReleaseChild(transaction, handle);
            return released.ok()
                       ? Result<MutableBuildView>(Resource(
                             "dynamic child local tracking allocation failed"))
                       : Result<MutableBuildView>(released);
        }
        std::memset(build->data, 0, build->object_size);
        return *build;
    }

    void MarkInactive(ShmHandle handle) noexcept {
        for (TrackedAllocation& allocation : allocations) {
            if (allocation.handle == handle) {
                allocation.active = false;
                return;
            }
        }
    }

    Status ReclaimHandles(std::span<const ShmHandle> handles) noexcept {
        for (auto it = handles.rbegin(); it != handles.rend(); ++it) {
            MINO_RETURN_IF_ERROR(journal->ReleaseChild(transaction, *it));
            MarkInactive(*it);
        }
        return Status::Ok();
    }

    Status TrackOwned(std::vector<ShmHandle>& owned,
                      ShmHandle handle) noexcept {
        try {
            owned.push_back(handle);
            return Status::Ok();
        } catch (const std::bad_alloc&) {
            const Status released = journal->ReleaseChild(transaction, handle);
            if (released.ok()) MarkInactive(handle);
            return released.ok()
                       ? Resource("dynamic child ownership tracking allocation failed")
                       : released;
        } catch (...) {
            return Status::Error(StatusCode::kInternal);
        }
    }

    Status BuildUnknownFields(const UnknownFieldSet& unknown_fields,
                              std::byte* metadata,
                              const SchemaDescriptor& owning_descriptor,
                              std::vector<ShmHandle>& owned) {
        std::memset(metadata, 0, VariableMetadataLayout::kSize);
        const auto fields = unknown_fields.fields();
        if (fields.size() > options.unknown_fields.max_fields ||
            unknown_fields.byte_size() > options.unknown_fields.max_bytes ||
            unknown_fields.byte_size() > (64u << 10) ||
            fields.size() > kDynamicUnknownFieldMaxCount) {
            return Resource("unknown fields exceed configured limits");
        }
        size_t payload_size = 0;
        for (const UnknownField& field : fields) {
            if (field.canonical_bytes().size() >
                    std::numeric_limits<uint32_t>::max() ||
                payload_size > std::numeric_limits<size_t>::max() - 8 -
                                   field.canonical_bytes().size()) {
                return Resource("unknown field framing size overflows");
            }
            payload_size += 8 + field.canonical_bytes().size();
        }
        Write64(metadata + VariableMetadataLayout::kLengthOffset,
                fields.size());
        Write64(metadata + VariableMetadataLayout::kCapacityOffset,
                payload_size);
        Write64(metadata + VariableMetadataLayout::kElementSizeOffset, 0);
        if (payload_size == 0) return Status::Ok();
        MINO_ASSIGN_OR_RETURN(
            MutableBuildView child,
            AllocateChild(payload_size, 8, owning_descriptor));
        MINO_RETURN_IF_ERROR(TrackOwned(owned, child.handle));
        auto* cursor = static_cast<std::byte*>(child.data);
        for (const UnknownField& field : fields) {
            Write32(cursor, field.field_id());
            Write32(cursor + 4,
                    static_cast<uint32_t>(field.canonical_bytes().size()));
            cursor += 8;
            std::memcpy(cursor, field.canonical_bytes().data(),
                        field.canonical_bytes().size());
            cursor += field.canonical_bytes().size();
        }
        WriteHandle(metadata + VariableMetadataLayout::kHandleOffset,
                    child.handle);
        return Status::Ok();
    }

    Status BuildMessage(const SchemaDescriptor& target,
                        const LayoutPlan& target_layout,
                        const DynamicMessage& message, std::byte* output,
                        size_t depth, std::vector<ShmHandle>& owned) {
        if (depth > options.graph_limits.max_depth) {
            return Resource("dynamic message nesting exceeds max_depth");
        }
        InitializeObject(target, target_layout, output);
        for (const DynamicField& supplied : message.fields()) {
            if (target.aggregate().FindField(supplied.id()) == nullptr) {
                return Mismatch("DynamicMessage contains an unknown field id");
            }
        }
        const auto fields = target.aggregate().fields();
        const auto field_layouts = target_layout.fields();
        for (size_t i = 0; i < fields.size(); ++i) {
            const DynamicValue* value = message.FindField(fields[i].id());
            if (value == nullptr) {
                if (fields[i].cardinality() == FieldCardinality::kOptional) continue;
                return Mismatch("required dynamic field is missing");
            }
            MINO_RETURN_IF_ERROR(BuildValue(fields[i].type(),
                                            fields[i].constraints(), *value,
                                            output + field_layouts[i].offset(),
                                            target, depth + 1, owned));
            SetPresence(target_layout, field_layouts[i], output);
        }
        if (target_layout.unknown_fields_offset().has_value()) {
            return BuildUnknownFields(
                message.unknown_fields(),
                output + *target_layout.unknown_fields_offset(), target, owned);
        }
        return message.unknown_fields().fields().empty()
                   ? Status::Ok()
                   : Mismatch("fixed struct cannot carry unknown fields");
    }

    Status BuildValue(const TypeDescriptor& type,
                      const ConstraintSet& constraints,
                      const DynamicValue& value, std::byte* output,
                      const SchemaDescriptor& owning_descriptor, size_t depth,
                      std::vector<ShmHandle>& owned) {
        if (depth > options.graph_limits.max_depth) {
            return Resource("dynamic value nesting exceeds max_depth");
        }
        if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
            const MessageValue* message = value.message();
            if (message == nullptr || message->value == nullptr) {
                return Mismatch("nested field requires a DynamicMessage");
            }
            MINO_ASSIGN_OR_RETURN(const SchemaDescriptor* nested,
                                  FindDescriptor(type.name()));
            MINO_ASSIGN_OR_RETURN(const LayoutPlan* nested_layout,
                                  FindPlan(*nested));
            if (nested->aggregate().kind() == AggregateKind::kStruct) {
                return BuildMessage(*nested, *nested_layout, *message->value,
                                    output, depth, owned);
            }
            MINO_ASSIGN_OR_RETURN(
                MutableBuildView child,
                AllocateChild(nested_layout->object_size(),
                              nested_layout->object_alignment(), *nested));
            MINO_RETURN_IF_ERROR(TrackOwned(owned, child.handle));
            MINO_RETURN_IF_ERROR(BuildMessage(
                *nested, *nested_layout, *message->value,
                static_cast<std::byte*>(child.data), depth, owned));
            WriteHandle(output + VariableMetadataLayout::kHandleOffset,
                        child.handle);
            Write64(output + VariableMetadataLayout::kLengthOffset,
                    nested_layout->object_size());
            Write64(output + VariableMetadataLayout::kCapacityOffset,
                    nested_layout->object_size());
            Write64(output + VariableMetadataLayout::kElementSizeOffset, 1);
            return Status::Ok();
        }
        if (type.kind() == TypeDescriptor::Kind::kVector) {
            const VectorValue* vector = value.vector();
            if (vector == nullptr || vector->value == nullptr ||
                type.element_type() == nullptr ||
                !constraints.max_capacity().has_value()) {
                return Mismatch("vector field requires a DynamicVector and max_capacity");
            }
            const size_t count = vector->value->values().size();
            if (count > *constraints.max_capacity()) {
                return Resource("vector exceeds descriptor max_capacity");
            }
            MINO_ASSIGN_OR_RETURN(ValueShape element,
                                  Shape(*type.element_type(), constraints));
            Write64(output + VariableMetadataLayout::kLengthOffset, count);
            Write64(output + VariableMetadataLayout::kCapacityOffset, count);
            Write64(output + VariableMetadataLayout::kElementSizeOffset,
                    element.size);
            if (count == 0) return Status::Ok();
            size_t bytes = 0;
            if (!CheckedMultiply(count, element.size, bytes)) {
                return Resource("vector allocation size overflows");
            }
            MINO_ASSIGN_OR_RETURN(
                MutableBuildView child,
                AllocateChild(bytes, element.alignment, owning_descriptor));
            MINO_RETURN_IF_ERROR(TrackOwned(owned, child.handle));
            auto* elements = static_cast<std::byte*>(child.data);
            for (size_t i = 0; i < count; ++i) {
                MINO_RETURN_IF_ERROR(BuildValue(
                    *type.element_type(), constraints,
                    vector->value->values()[i], elements + i * element.size,
                    owning_descriptor, depth + 1, owned));
            }
            WriteHandle(output + VariableMetadataLayout::kHandleOffset,
                        child.handle);
            return Status::Ok();
        }
        if (!type.scalar().has_value()) return Mismatch("incomplete scalar type");
        switch (*type.scalar()) {
            case ScalarType::kInt32: {
                const auto* number = value.signed_integer();
                if (number == nullptr ||
                    number->value < std::numeric_limits<int32_t>::min() ||
                    number->value > std::numeric_limits<int32_t>::max()) {
                    return Mismatch("int32 value has wrong type/range");
                }
                Write32(output, static_cast<uint32_t>(number->value));
                return Status::Ok();
            }
            case ScalarType::kInt64: {
                const auto* number = value.signed_integer();
                if (number == nullptr) return Mismatch("int64 value has wrong type");
                Write64(output, static_cast<uint64_t>(number->value));
                return Status::Ok();
            }
            case ScalarType::kUint32:
            case ScalarType::kFixed32: {
                const auto* number = value.unsigned_integer();
                if (number == nullptr ||
                    number->value > std::numeric_limits<uint32_t>::max()) {
                    return Mismatch("32-bit unsigned value has wrong type/range");
                }
                Write32(output, static_cast<uint32_t>(number->value));
                return Status::Ok();
            }
            case ScalarType::kUint64:
            case ScalarType::kFixed64: {
                const auto* number = value.unsigned_integer();
                if (number == nullptr) return Mismatch("64-bit unsigned value has wrong type");
                Write64(output, number->value);
                return Status::Ok();
            }
            case ScalarType::kFloat: {
                const auto* number = value.float32();
                if (number == nullptr) return Mismatch("float requires exact bits");
                Write32(output, number->bits);
                return Status::Ok();
            }
            case ScalarType::kDouble: {
                const auto* number = value.float64();
                if (number == nullptr) return Mismatch("double requires exact bits");
                Write64(output, number->bits);
                return Status::Ok();
            }
            case ScalarType::kBool: {
                const auto* boolean = value.boolean();
                if (boolean == nullptr) return Mismatch("bool value has wrong type");
                output[0] = boolean->value ? std::byte{1} : std::byte{0};
                return Status::Ok();
            }
            case ScalarType::kString:
            case ScalarType::kBytes: {
                std::span<const std::byte> bytes;
                if (*type.scalar() == ScalarType::kString) {
                    const auto* string = value.string();
                    if (string == nullptr) return Mismatch("string value has wrong type");
                    bytes = std::as_bytes(std::span(string->value));
                    if (!IsValidUtf8(bytes)) return Mismatch("string is not valid UTF-8");
                } else {
                    const auto* raw = value.bytes();
                    if (raw == nullptr) return Mismatch("bytes value has wrong type");
                    bytes = raw->value;
                }
                if (!constraints.max_bytes().has_value()) {
                    return Mismatch("dynamic bytes field is missing max_bytes");
                }
                if (bytes.size() > *constraints.max_bytes()) {
                    return Resource("dynamic bytes exceeds max_bytes");
                }
                Write64(output + VariableMetadataLayout::kLengthOffset,
                        bytes.size());
                Write64(output + VariableMetadataLayout::kCapacityOffset,
                        bytes.size());
                Write64(output + VariableMetadataLayout::kElementSizeOffset, 1);
                if (bytes.empty()) return Status::Ok();
                MINO_ASSIGN_OR_RETURN(
                    MutableBuildView child,
                    AllocateChild(bytes.size(), 1, owning_descriptor));
                MINO_RETURN_IF_ERROR(TrackOwned(owned, child.handle));
                std::memcpy(child.data, bytes.data(), bytes.size());
                WriteHandle(output + VariableMetadataLayout::kHandleOffset,
                            child.handle);
                return Status::Ok();
            }
        }
        return Mismatch("unsupported scalar type");
    }

    static void SetPresence(const LayoutPlan& target_layout,
                            const FieldLayout& field,
                            std::byte* object) noexcept {
        if (!field.presence_bit().has_value()) return;
        const size_t bit = *field.presence_bit();
        std::byte* word = object + target_layout.presence_bitmap_offset() +
                          (bit / 64) * 8;
        Write64(word, Read64(word) | (uint64_t{1} << (bit % 64)));
    }

    Status ValidateFieldHandle(const FieldHandle& field,
                               size_t& index) const noexcept {
        if (field.digest_ != descriptor->identity().canonical_digest() ||
            field.schema_short_id_ != descriptor->identity().short_id() ||
            field.layout_version_ != layout.layout_version() ||
            field.field_index_ >= descriptor->aggregate().fields().size() ||
            descriptor->aggregate().fields()[field.field_index_].id() !=
                field.field_id_) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "FieldHandle belongs to a different schema/layout");
        }
        index = field.field_index_;
        return Status::Ok();
    }

    Status SetIndex(size_t index, const DynamicValue& value) {
        if (!transaction_active) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "dynamic builder is not active");
        }
        const auto fields = descriptor->aggregate().fields();
        const auto field_layouts = layout.fields();
        if (index >= fields.size() || index >= field_layouts.size()) {
            return Status::Error(StatusCode::kNotFound,
                                 "dynamic field index is out of range");
        }
        const FieldLayout& field_layout = field_layouts[index];
        std::vector<std::byte> encoded(field_layout.size());
        std::vector<ShmHandle> newly_owned;
        Status built = Status::Ok();
        try {
            built = BuildValue(fields[index].type(),
                               fields[index].constraints(), value,
                               encoded.data(), *descriptor, 0,
                               newly_owned);
        } catch (const std::bad_alloc&) {
            (void)ReclaimHandles(newly_owned);
            return Resource("dynamic value construction allocation failed");
        } catch (...) {
            (void)ReclaimHandles(newly_owned);
            return Status::Error(StatusCode::kInternal);
        }
        if (!built.ok()) {
            (void)ReclaimHandles(newly_owned);
            return built;
        }
        std::memcpy(root_data + field_layout.offset(), encoded.data(),
                    encoded.size());
        SetPresence(layout, field_layout, root_data);
        std::vector<ShmHandle> old = std::move(field_allocations[index]);
        field_allocations[index] = std::move(newly_owned);
        fields_set[index] = true;
        return ReclaimHandles(old);
    }
};

struct DynamicView::Context {
    const CentralSlabAllocator* allocator = nullptr;
    ShmHandle root_handle{};
    ShmPinToken root_pin;
    DynamicObjectOptions options;
    uint64_t owner_epoch = 0;
    uint64_t allocation_transaction_id = 0;
    std::vector<std::shared_ptr<const SchemaDescriptor>> descriptor_owners;
    std::map<std::string, const SchemaDescriptor*, std::less<>> descriptors;
    std::map<std::string, LayoutPlan, std::less<>> layouts;

    const SchemaDescriptor* FindDescriptor(std::string_view name) const noexcept {
        const auto it = descriptors.find(name);
        return it == descriptors.end() ? nullptr : it->second;
    }
    const LayoutPlan* FindLayout(std::string_view name) const noexcept {
        const auto it = layouts.find(name);
        return it == layouts.end() ? nullptr : &it->second;
    }
};

namespace {

Result<size_t> ResolveHandle(const SchemaDescriptor& descriptor,
                             const LayoutPlan& layout,
                             const FieldHandle& field) noexcept {
    if (field.schema_digest() != descriptor.identity().canonical_digest() ||
        field.schema_short_id() != descriptor.identity().short_id() ||
        field.layout_version() != layout.layout_version() ||
        field.field_index() >= descriptor.aggregate().fields().size() ||
        descriptor.aggregate().fields()[field.field_index()].id() !=
            field.field_id()) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "FieldHandle belongs to a different schema/layout");
    }
    return field.field_index();
}

Result<SlabView> InspectContextAllocation(
    const DynamicView::Context& context, ShmHandle handle) {
    if (handle.IsNull()) return Corrupt("dynamic view storage handle is null");
    auto slab = context.allocator->Inspect(handle);
    if (!slab.ok()) return slab.status();
    const bool is_root = handle == context.root_handle;
    if ((is_root && slab->state != ObjectState::kPublished &&
         slab->state != ObjectState::kRetired) ||
        (!is_root && slab->state != ObjectState::kPublished)) {
        return Corrupt("dynamic view allocation is not safely readable");
    }
    if (slab->owner_epoch != context.owner_epoch ||
        slab->allocation_transaction_id != context.allocation_transaction_id ||
        slab->allocation_flags !=
            (is_root ? kAllocationFlagTransactionRoot
                     : kAllocationFlagTransactionChild)) {
        return Corrupt("dynamic view allocation ownership changed");
    }
    if (is_root &&
        (!context.root_pin.active() ||
         context.root_pin.handle() != context.root_handle ||
         context.root_pin.data() == nullptr)) {
        return Corrupt("dynamic root Pin capability is no longer active");
    }
    return *slab;
}

Result<const std::byte*> ViewData(const DynamicView& view) {
    const auto& context = view.context_for_internal();
    const ShmHandle storage = view.storage_handle_for_internal();
    MINO_ASSIGN_OR_RETURN(SlabView slab,
                          InspectContextAllocation(*context, storage));
    const size_t offset = view.storage_offset_for_internal();
    if (offset > slab.object_size ||
        view.layout().object_size() > slab.object_size - offset) {
        return Corrupt("dynamic inline object is outside its allocation");
    }
    const auto* base = storage == context->root_handle
                           ? static_cast<const std::byte*>(
                                 context->root_pin.data())
                           : static_cast<const std::byte*>(slab.data);
    return base + offset;
}

bool IsPresent(const LayoutPlan& layout, const FieldLayout& field,
               const std::byte* data) noexcept {
    if (!field.presence_bit().has_value()) return true;
    const size_t bit = *field.presence_bit();
    return (Read64(data + layout.presence_bitmap_offset() + (bit / 64) * 8) &
            (uint64_t{1} << (bit % 64))) != 0;
}

Result<size_t> FieldStorageOffset(const DynamicView& view, size_t index) {
    if (index >= view.descriptor().aggregate().fields().size() ||
        index >= view.layout().fields().size()) {
        return Status::Error(StatusCode::kNotFound, "field index is out of range");
    }
    const FieldLayout& field = view.layout().fields()[index];
    if (field.offset() > view.layout().object_size() ||
        field.size() > view.layout().object_size() - field.offset()) {
        return Corrupt("dynamic field layout is outside its object");
    }
    size_t offset = 0;
    if (!CheckedAdd(view.storage_offset_for_internal(), field.offset(), offset)) {
        return Corrupt("dynamic field storage offset overflows");
    }
    return offset;
}

Result<const std::byte*> FieldData(const DynamicView& view, size_t index,
                                   bool require_present = true) {
    MINO_ASSIGN_OR_RETURN(size_t unused_offset,
                          FieldStorageOffset(view, index));
    (void)unused_offset;
    MINO_ASSIGN_OR_RETURN(const std::byte* data, ViewData(view));
    const FieldLayout& field = view.layout().fields()[index];
    if (require_present && !IsPresent(view.layout(), field, data)) {
        return Status::Error(StatusCode::kNotFound, "optional field is absent");
    }
    return data + field.offset();
}

struct VariableAccess {
    ShmHandle handle{};
    const std::byte* data = nullptr;
    size_t length = 0;
    size_t capacity = 0;
    size_t element_size = 0;
};

Result<VariableAccess> ResolveVariable(
    const DynamicView::Context& context, ShmHandle metadata_handle,
    size_t metadata_offset) {
    MINO_ASSIGN_OR_RETURN(
        SlabView owner,
        InspectContextAllocation(context, metadata_handle));
    if (metadata_offset > owner.object_size ||
        VariableMetadataLayout::kSize > owner.object_size - metadata_offset) {
        return Corrupt("dynamic variable metadata is outside its allocation");
    }
    const auto* owner_data = metadata_handle == context.root_handle
                                 ? static_cast<const std::byte*>(
                                       context.root_pin.data())
                                 : static_cast<const std::byte*>(owner.data);
    const std::byte* metadata = owner_data + metadata_offset;
    const ShmHandle child = ReadHandle(metadata);
    const uint64_t length64 =
        Read64(metadata + VariableMetadataLayout::kLengthOffset);
    const uint64_t capacity64 =
        Read64(metadata + VariableMetadataLayout::kCapacityOffset);
    const uint64_t element_size64 =
        Read64(metadata + VariableMetadataLayout::kElementSizeOffset);
    if (length64 > capacity64 ||
        length64 > std::numeric_limits<size_t>::max() ||
        capacity64 > std::numeric_limits<size_t>::max() ||
        element_size64 > std::numeric_limits<size_t>::max()) {
        return Corrupt("dynamic variable metadata exceeds process bounds");
    }
    const size_t length = static_cast<size_t>(length64);
    const size_t capacity = static_cast<size_t>(capacity64);
    const size_t element_size = static_cast<size_t>(element_size64);
    if (capacity == 0) {
        if (!child.IsNull() || length != 0) {
            return Corrupt("empty dynamic variable has child storage");
        }
        return VariableAccess{.length = 0,
                              .capacity = 0,
                              .element_size = element_size};
    }
    if (child.IsNull() || element_size == 0) {
        return Corrupt("non-empty dynamic variable metadata is invalid");
    }
    size_t bytes = 0;
    if (!CheckedMultiply(capacity, element_size, bytes) ||
        bytes > std::numeric_limits<uint32_t>::max()) {
        return Corrupt("dynamic variable byte size overflows");
    }
    MINO_ASSIGN_OR_RETURN(SlabView slab,
                          InspectContextAllocation(context, child));
    if (slab.object_size != bytes) {
        return Corrupt("dynamic variable child size changed");
    }
    return VariableAccess{.handle = child,
                          .data = static_cast<const std::byte*>(slab.data),
                          .length = length,
                          .capacity = capacity,
                          .element_size = element_size};
}

Result<std::span<const std::byte>> VariableBytes(
    const DynamicView::Context& context, ShmHandle metadata_handle,
    size_t metadata_offset, size_t max_bytes) {
    MINO_ASSIGN_OR_RETURN(
        VariableAccess value,
        ResolveVariable(context, metadata_handle, metadata_offset));
    if (value.element_size != 1 || value.capacity > max_bytes) {
        return Corrupt("dynamic bytes metadata violates descriptor constraints");
    }
    if (value.length == 0) return std::span<const std::byte>{};
    return std::span(value.data, value.length);
}

Result<size_t> ViewElementSize(const DynamicView::Context& context,
                               const TypeDescriptor& type) {
    if (type.kind() == TypeDescriptor::Kind::kVector) {
        return VariableMetadataLayout::kSize;
    }
    if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
        const SchemaDescriptor* descriptor = context.FindDescriptor(type.name());
        const LayoutPlan* layout = context.FindLayout(type.name());
        if (descriptor == nullptr || layout == nullptr) {
            return Status::Error(StatusCode::kNotFound,
                                 "dynamic element descriptor is unavailable");
        }
        return descriptor->aggregate().kind() == AggregateKind::kStruct
                   ? layout->object_size()
                   : VariableMetadataLayout::kSize;
    }
    if (!type.scalar().has_value()) return Corrupt("incomplete scalar element");
    switch (*type.scalar()) {
        case ScalarType::kBool:
            return 1;
        case ScalarType::kInt32:
        case ScalarType::kUint32:
        case ScalarType::kFixed32:
        case ScalarType::kFloat:
            return 4;
        case ScalarType::kInt64:
        case ScalarType::kUint64:
        case ScalarType::kFixed64:
        case ScalarType::kDouble:
            return 8;
        case ScalarType::kString:
        case ScalarType::kBytes:
            return VariableMetadataLayout::kSize;
    }
    return Corrupt("unsupported dynamic element type");
}

}  // namespace

Result<FieldHandle> FieldHandle::ByName(const SchemaDescriptor& descriptor,
                                        const LayoutPlan& layout,
                                        std::string_view name) noexcept {
    try {
        const auto fields = descriptor.aggregate().fields();
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i].name() == name) return ByIndex(descriptor, layout, i);
        }
        return Status::Error(StatusCode::kNotFound, "field name was not found");
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<FieldHandle> FieldHandle::ById(const SchemaDescriptor& descriptor,
                                      const LayoutPlan& layout,
                                      uint32_t field_id) noexcept {
    const auto fields = descriptor.aggregate().fields();
    const auto it = std::lower_bound(fields.begin(), fields.end(), field_id,
                                     [](const FieldDescriptor& field, uint32_t id) {
                                         return field.id() < id;
                                     });
    if (it == fields.end() || it->id() != field_id) {
        return Status::Error(StatusCode::kNotFound, "field id was not found");
    }
    return ByIndex(descriptor, layout,
                   static_cast<size_t>(it - fields.begin()));
}

Result<FieldHandle> FieldHandle::ByIndex(const SchemaDescriptor& descriptor,
                                         const LayoutPlan& layout,
                                         size_t field_index) noexcept {
    const auto fields = descriptor.aggregate().fields();
    if (field_index >= fields.size() || field_index >= layout.fields().size() ||
        fields[field_index].id() != layout.fields()[field_index].field_id()) {
        return Status::Error(StatusCode::kNotFound,
                             "field index is not represented by LayoutPlan");
    }
    return FieldHandle(descriptor.identity().canonical_digest(),
                       descriptor.identity().short_id(), layout.layout_version(),
                       fields[field_index].id(), field_index);
}

Result<DynamicBuilder> DynamicBuilder::Create(
    DynamicSchemaHandle descriptor, LayoutPlan layout,
    CentralSlabAllocator& allocator, AllocationJournal& journal, TypeId type_id,
    std::span<const DynamicSchemaHandle> descriptors,
    const ProcessIdentity& owner, const DynamicObjectOptions& options) noexcept {
    try {
        if (descriptor == nullptr || owner.IsZero() || layout.object_size() == 0 ||
            layout.object_size() > std::numeric_limits<uint32_t>::max() ||
            descriptor->identity().short_id() == 0 ||
            DigestShortId(descriptor->identity().canonical_digest()) !=
                descriptor->identity().short_id() ||
            descriptor->identity().layout_version() != layout.layout_version()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "invalid dynamic builder schema/layout capability");
        }
        MINO_RETURN_IF_ERROR(
            ValidateDescriptorClosure(*descriptor, descriptors));
        MINO_RETURN_IF_ERROR(
            LayoutPlanner::Validate(*descriptor, layout, descriptors));
        auto impl = std::make_unique<Impl>(std::move(layout));
        impl->descriptor_owner = std::move(descriptor);
        impl->descriptor = impl->descriptor_owner.get();
        impl->allocator = &allocator;
        impl->journal = &journal;
        impl->type_id = type_id;
        impl->options = options;
        impl->descriptor_owners.assign(descriptors.begin(), descriptors.end());
        const bool root_is_owned = std::any_of(
            impl->descriptor_owners.begin(), impl->descriptor_owners.end(),
            [&](const DynamicSchemaHandle& candidate) {
                return candidate != nullptr &&
                       candidate->aggregate().full_name() ==
                           impl->descriptor->aggregate().full_name() &&
                       candidate->identity().canonical_digest() ==
                           impl->descriptor->identity().canonical_digest();
            });
        if (!root_is_owned) {
            impl->descriptor_owners.push_back(impl->descriptor_owner);
        }
        impl->descriptors.emplace(
            std::string(impl->descriptor->aggregate().full_name()),
            impl->descriptor);
        impl->layouts.emplace(
            std::string(impl->descriptor->aggregate().full_name()), impl->layout);
        for (const auto& dependency : impl->descriptor_owners) {
            if (dependency == nullptr) continue;
            if (DigestShortId(dependency->identity().canonical_digest()) !=
                dependency->identity().short_id()) {
                return Status::Error(StatusCode::kSchemaMismatch,
                                     "descriptor short ID does not match digest");
            }
            for (const auto& [unused, existing] : impl->descriptors) {
                (void)unused;
                if (((existing->identity().short_id() ==
                          dependency->identity().short_id()) ||
                     (existing->aggregate().full_name() ==
                          dependency->aggregate().full_name())) &&
                    existing->identity().canonical_digest() !=
                        dependency->identity().canonical_digest()) {
                    return Status::Error(StatusCode::kSchemaMismatch,
                                         "schema short ID collision in dynamic closure");
                }
            }
            impl->descriptors.insert_or_assign(
                std::string(dependency->aggregate().full_name()),
                dependency.get());
        }
        MINO_ASSIGN_OR_RETURN(AllocationTransaction transaction,
                              journal.Begin(owner));
        AllocationRequest request;
        request.object_size = static_cast<uint32_t>(impl->layout.object_size());
        request.type_id = type_id;
        request.schema = mino::SchemaIdentity{
            .short_id = impl->descriptor->identity().short_id(),
            .layout_version = impl->descriptor->identity().layout_version()};
        request.alignment =
            static_cast<uint32_t>(impl->layout.object_alignment());
        auto allocated = journal.AllocateRoot(transaction, request);
        if (!allocated.ok()) {
            (void)journal.Abort(transaction);
            return allocated.status();
        }
        const ShmHandle root = *allocated;
        auto build = allocator.BeginBuild(root);
        if (!build.ok()) {
            (void)journal.Abort(transaction);
            return build.status();
        }
        impl->root = root;
        impl->root_data = static_cast<std::byte*>(build->data);
        impl->transaction = transaction;
        impl->transaction_active = true;
        impl->allocations.push_back(Impl::TrackedAllocation{root, true});
        impl->field_allocations.resize(
            impl->descriptor->aggregate().fields().size());
        impl->fields_set.resize(
            impl->descriptor->aggregate().fields().size(), false);
        InitializeObject(*impl->descriptor, impl->layout, impl->root_data);
        return DynamicBuilder(std::move(impl));
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<DynamicBuilder> DynamicBuilder::FromDynamicMessage(
    DynamicSchemaHandle descriptor, LayoutPlan layout,
    const DynamicMessage& message, CentralSlabAllocator& allocator,
    AllocationJournal& journal, TypeId type_id,
    std::span<const DynamicSchemaHandle> descriptors,
    const ProcessIdentity& owner, const DynamicObjectOptions& options) noexcept {
    auto builder = Create(std::move(descriptor), std::move(layout), allocator,
                          journal, type_id, descriptors, owner, options);
    if (!builder.ok()) return builder.status();
    const Status set = builder->SetFromDynamicMessage(message);
    if (!set.ok()) {
        (void)builder->Abort();
        return set;
    }
    return std::move(*builder);
}

DynamicBuilder::DynamicBuilder(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}
DynamicBuilder::DynamicBuilder(DynamicBuilder&& other) noexcept = default;
DynamicBuilder& DynamicBuilder::operator=(DynamicBuilder&& other) noexcept {
    if (this != &other) {
        if (impl_ != nullptr && impl_->transaction_active) (void)Abort();
        impl_ = std::move(other.impl_);
    }
    return *this;
}
DynamicBuilder::~DynamicBuilder() noexcept {
    if (impl_ != nullptr && impl_->transaction_active) (void)Abort();
}
ShmHandle DynamicBuilder::root_handle() const noexcept {
    return impl_ == nullptr ? ShmHandle{} : impl_->root;
}
bool DynamicBuilder::active() const noexcept {
    return impl_ != nullptr && impl_->transaction_active;
}

Status DynamicBuilder::Set(const FieldHandle& field,
                           const DynamicValue& value) noexcept {
    try {
        if (impl_ == nullptr) return Status::Error(StatusCode::kInvalidArgument);
        size_t index = 0;
        MINO_RETURN_IF_ERROR(impl_->ValidateFieldHandle(field, index));
        return impl_->SetIndex(index, value);
    } catch (const std::bad_alloc&) {
        return Resource("dynamic field update allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}
Status DynamicBuilder::SetById(uint32_t field_id,
                               const DynamicValue& value) noexcept {
    if (impl_ == nullptr) return Status::Error(StatusCode::kInvalidArgument);
    auto field = FieldHandle::ById(*impl_->descriptor, impl_->layout, field_id);
    return field.ok() ? Set(*field, value) : field.status();
}
Status DynamicBuilder::SetByIndex(size_t field_index,
                                  const DynamicValue& value) noexcept {
    try {
        return impl_ == nullptr
                   ? Status::Error(StatusCode::kInvalidArgument)
                   : impl_->SetIndex(field_index, value);
    } catch (const std::bad_alloc&) {
        return Resource("dynamic field update allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}
Status DynamicBuilder::SetFromDynamicMessage(
    const DynamicMessage& message) noexcept {
    try {
        if (impl_ == nullptr) return Status::Error(StatusCode::kInvalidArgument);
        for (const DynamicField& field : message.fields()) {
            MINO_RETURN_IF_ERROR(SetById(field.id(), field.value()));
        }
        if (!impl_->layout.unknown_fields_offset().has_value()) {
            return message.unknown_fields().fields().empty()
                       ? Status::Ok()
                       : Mismatch("fixed struct cannot carry unknown fields");
        }
        std::byte encoded[VariableMetadataLayout::kSize] = {};
        std::vector<ShmHandle> newly_owned;
        const Status built = impl_->BuildUnknownFields(
            message.unknown_fields(), encoded, *impl_->descriptor, newly_owned);
        if (!built.ok()) {
            (void)impl_->ReclaimHandles(newly_owned);
            return built;
        }
        std::memcpy(impl_->root_data + *impl_->layout.unknown_fields_offset(),
                    encoded, sizeof(encoded));
        std::vector<ShmHandle> old = std::move(impl_->unknown_allocations);
        impl_->unknown_allocations = std::move(newly_owned);
        return impl_->ReclaimHandles(old);
    } catch (const std::bad_alloc&) {
        return Resource("dynamic message conversion allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}
Status DynamicBuilder::SetSigned(const FieldHandle& field, int64_t value) noexcept {
    return Set(field, DynamicValue::Signed(value));
}
Status DynamicBuilder::SetUnsigned(const FieldHandle& field,
                                   uint64_t value) noexcept {
    return Set(field, DynamicValue::Unsigned(value));
}
Status DynamicBuilder::SetBool(const FieldHandle& field, bool value) noexcept {
    return Set(field, DynamicValue::Boolean(value));
}
Status DynamicBuilder::SetFixed32(const FieldHandle& field,
                                  uint32_t value) noexcept {
    return SetUnsigned(field, value);
}
Status DynamicBuilder::SetFixed64(const FieldHandle& field,
                                  uint64_t value) noexcept {
    return SetUnsigned(field, value);
}
Status DynamicBuilder::SetFloat32Bits(const FieldHandle& field,
                                      uint32_t bits) noexcept {
    return Set(field, DynamicValue::Float32Bits(bits));
}
Status DynamicBuilder::SetFloat64Bits(const FieldHandle& field,
                                      uint64_t bits) noexcept {
    return Set(field, DynamicValue::Float64Bits(bits));
}
Status DynamicBuilder::SetString(const FieldHandle& field,
                                 std::string_view value) noexcept {
    auto dynamic = DynamicValue::String(value);
    return dynamic.ok() ? Set(field, *dynamic) : dynamic.status();
}
Status DynamicBuilder::SetBytes(const FieldHandle& field,
                                std::span<const std::byte> value) noexcept {
    auto dynamic = DynamicValue::Bytes(value);
    return dynamic.ok() ? Set(field, *dynamic) : dynamic.status();
}
Status DynamicBuilder::SetVector(const FieldHandle& field,
                                 const DynamicVector& value) noexcept {
    try {
        auto copy = std::make_shared<DynamicVector>(value);
        auto dynamic = DynamicValue::Vector(std::move(copy));
        return dynamic.ok() ? Set(field, *dynamic) : dynamic.status();
    } catch (const std::bad_alloc&) {
        return Resource("vector conversion allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}
Status DynamicBuilder::SetNested(const FieldHandle& field,
                                 const DynamicMessage& value) noexcept {
    try {
        auto copy = std::make_shared<DynamicMessage>(value);
        auto dynamic = DynamicValue::Message(std::move(copy));
        return dynamic.ok() ? Set(field, *dynamic) : dynamic.status();
    } catch (const std::bad_alloc&) {
        return Resource("nested conversion allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<PreparedDynamicObject> DynamicBuilder::Prepare() noexcept {
    try {
        if (impl_ == nullptr || !impl_->transaction_active) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "dynamic builder is not active");
        }
        const auto fields = impl_->descriptor->aggregate().fields();
        for (size_t i = 0; i < fields.size(); ++i) {
            if (fields[i].cardinality() != FieldCardinality::kOptional &&
                !impl_->fields_set[i]) {
                return Mismatch("required dynamic field was not set");
            }
        }
        ObjectGraphWalkOptions walk_options;
        walk_options.limits = impl_->options.graph_limits;
        walk_options.unknown_fields = impl_->options.unknown_fields;
        walk_options.allow_building = true;
        auto reachable = ObjectGraphWalker::Collect(
            *impl_->descriptor, impl_->layout, impl_->root, *impl_->allocator,
            impl_->descriptor_owners, walk_options);
        if (!reachable.ok()) return reachable.status();
        MINO_RETURN_IF_ERROR(impl_->journal->ValidateManifest(
            impl_->transaction, *reachable));
        const Status published = impl_->journal->PublishGraph(impl_->transaction);
        if (!published.ok()) {
            (void)impl_->journal->Abort(impl_->transaction);
            impl_->transaction_active = false;
            return published;
        }
        PreparedDynamicObject prepared(impl_->journal, impl_->transaction,
                                       impl_->root);
        impl_->transaction_active = false;
        return prepared;
    } catch (const std::bad_alloc&) {
        return Resource("dynamic prepare validation allocation failed");
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<DynamicObject> DynamicBuilder::Commit(ShmPinTable& pins) noexcept {
    if (impl_ == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "dynamic builder is not active");
    }
    const TypeId type_id = impl_->type_id;
    const uint64_t short_id = impl_->descriptor->identity().short_id();
    const uint32_t layout_version = impl_->layout.layout_version();
    const uint32_t object_size = static_cast<uint32_t>(impl_->layout.object_size());
    AllocationJournal* journal = impl_->journal;
    auto prepared = Prepare();
    if (!prepared.ok()) return prepared.status();
    const PublicationBinding standalone{
        .channel_kind = PublicationChannelKind::kNone,
        .payload = prepared->root_handle(),
    };
    const Status committed = prepared->CommitPublication(standalone);
    if (!committed.ok()) return committed;
    const AllocationTransaction transaction = prepared->transaction();
    const ShmHandle root = prepared->root_handle();
    prepared->Disarm();
    return DynamicObject(
        journal, &pins, transaction, root,
        ShmPinContract{.type_id = type_id,
                       .schema_short_id = short_id,
                       .layout_version = layout_version,
                       .object_size = object_size});
}

Status DynamicBuilder::Abort() noexcept {
    if (impl_ == nullptr || !impl_->transaction_active) return Status::Ok();
    const Status status = impl_->journal->Abort(impl_->transaction);
    if (status.ok() || status.code() == StatusCode::kNotFound) {
        impl_->transaction_active = false;
    }
    return status;
}

PreparedDynamicObject::PreparedDynamicObject(
    PreparedDynamicObject&& other) noexcept {
    MoveFrom(other);
}
PreparedDynamicObject& PreparedDynamicObject::operator=(
    PreparedDynamicObject&& other) noexcept {
    if (this != &other) {
        if (active_) (void)Rollback();
        MoveFrom(other);
    }
    return *this;
}
PreparedDynamicObject::~PreparedDynamicObject() noexcept {
    if (active_) (void)Rollback();
}
void PreparedDynamicObject::MoveFrom(PreparedDynamicObject& other) noexcept {
    journal_ = other.journal_;
    transaction_ = other.transaction_;
    root_ = other.root_;
    active_ = other.active_;
    journal_committed_ = other.journal_committed_;
    other.Disarm();
}
void PreparedDynamicObject::Disarm() noexcept {
    journal_ = nullptr;
    transaction_ = {};
    root_ = {};
    active_ = false;
    journal_committed_ = false;
}
Status PreparedDynamicObject::CommitPublication(
    const PublicationBinding& binding) noexcept {
    if (!active_ || journal_committed_ || journal_ == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "prepared dynamic object is not bindable");
    }
    PublicationBinding stored = binding;
    if (stored.payload.IsNull()) stored.payload = root_;
    if (stored.payload != root_) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "publication binding payload is not dynamic root");
    }
    const Status status = journal_->Commit(transaction_, stored);
    if (status.ok()) journal_committed_ = true;
    return status;
}
Status PreparedDynamicObject::FinalizeVisible() noexcept {
    if (!active_ || !journal_committed_ || journal_ == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "prepared dynamic object is not committed");
    }
    const Status status = journal_->FinalizeCommit(transaction_);
    if (status.ok()) Disarm();
    return status;
}
Status PreparedDynamicObject::Rollback() noexcept {
    if (!active_ || journal_ == nullptr) return Status::Ok();
    const Status status = journal_committed_
                              ? journal_->RollbackCommitted(transaction_)
                              : journal_->Abort(transaction_);
    if (status.ok() || status.code() == StatusCode::kNotFound) Disarm();
    return status;
}

DynamicObject::DynamicObject(DynamicObject&& other) noexcept { MoveFrom(other); }
DynamicObject& DynamicObject::operator=(DynamicObject&& other) noexcept {
    if (this != &other) {
        if (active_) (void)Reclaim();
        MoveFrom(other);
    }
    return *this;
}
DynamicObject::~DynamicObject() noexcept {
    if (active_) (void)Reclaim();
}
void DynamicObject::MoveFrom(DynamicObject& other) noexcept {
    journal_ = other.journal_;
    pins_ = other.pins_;
    transaction_ = other.transaction_;
    root_ = other.root_;
    contract_ = other.contract_;
    active_ = other.active_;
    retired_ = other.retired_;
    other.journal_ = nullptr;
    other.pins_ = nullptr;
    other.transaction_ = {};
    other.root_ = {};
    other.active_ = false;
    other.retired_ = false;
}
Result<ShmPinToken> DynamicObject::Pin(
    const ProcessIdentity& owner) noexcept {
    if (!active_ || retired_ || pins_ == nullptr) {
        return Status::Error(StatusCode::kInvalidArgument,
                             "dynamic object is not pinnable");
    }
    return pins_->Pin(root_, contract_, owner);
}
Status DynamicObject::Reclaim() noexcept {
    if (!active_) return Status::Ok();
    if (pins_ == nullptr || journal_ == nullptr) {
        return Status::Error(StatusCode::kInternal,
                             "dynamic object ownership capability is incomplete");
    }
    if (!retired_) {
        MINO_RETURN_IF_ERROR(pins_->RetirePayload(root_));
        retired_ = true;
    }
    if (pins_->PinCount(root_) != 0) {
        return Status::Error(
            StatusCode::kUnavailable,
            "dynamic graph reclamation is deferred until all root Pins release");
    }
    const Status reclaimed = journal_->RollbackCommitted(transaction_);
    if (reclaimed.ok()) {
        active_ = false;
        journal_ = nullptr;
        pins_ = nullptr;
        transaction_ = {};
    }
    return reclaimed;
}

Result<DynamicView> DynamicView::Create(
    DynamicSchemaHandle descriptor, LayoutPlan layout, ShmHandle root,
    const CentralSlabAllocator& allocator, ShmPinToken root_pin,
    std::span<const DynamicSchemaHandle> descriptors,
    const DynamicObjectOptions& options) noexcept {
    try {
        if (descriptor == nullptr || !root_pin.active() ||
            root_pin.handle() != root || root_pin.data() == nullptr ||
            DigestShortId(descriptor->identity().canonical_digest()) !=
                descriptor->identity().short_id()) {
            return Status::Error(StatusCode::kSchemaMismatch,
                                 "DynamicView requires matching schema and Pin capabilities");
        }
        ObjectGraphWalkOptions walk_options;
        walk_options.limits = options.graph_limits;
        walk_options.unknown_fields = options.unknown_fields;
        MINO_RETURN_IF_ERROR(ObjectGraphWalker::Validate(
            *descriptor, layout, root, allocator, descriptors, walk_options));
        auto root_slab = allocator.Inspect(root);
        if (!root_slab.ok()) return root_slab.status();
        auto context = std::make_shared<Context>();
        context->allocator = &allocator;
        context->root_handle = root;
        context->root_pin = std::move(root_pin);
        context->options = options;
        context->owner_epoch = root_slab->owner_epoch;
        context->allocation_transaction_id =
            root_slab->allocation_transaction_id;
        context->descriptor_owners.assign(descriptors.begin(), descriptors.end());
        const bool root_is_owned = std::any_of(
            context->descriptor_owners.begin(), context->descriptor_owners.end(),
            [&](const DynamicSchemaHandle& candidate) {
                return candidate != nullptr &&
                       candidate->aggregate().full_name() ==
                           descriptor->aggregate().full_name() &&
                       candidate->identity().canonical_digest() ==
                           descriptor->identity().canonical_digest();
            });
        if (!root_is_owned) context->descriptor_owners.push_back(descriptor);
        context->descriptors.emplace(
            std::string(descriptor->aggregate().full_name()), descriptor.get());
        context->layouts.emplace(std::string(descriptor->aggregate().full_name()),
                                 std::move(layout));
        for (const auto& dependency : context->descriptor_owners) {
            if (dependency == nullptr) continue;
            context->descriptors.insert_or_assign(
                std::string(dependency->aggregate().full_name()), dependency.get());
        }
        for (const auto& [name, dependency] : context->descriptors) {
            if (context->layouts.contains(name)) continue;
            std::vector<DynamicSchemaHandle> closure;
            closure.reserve(dependency->dependencies().size());
            for (const DependencyDescriptor& required :
                 dependency->dependencies()) {
                const auto owner = std::find_if(
                    context->descriptor_owners.begin(),
                    context->descriptor_owners.end(),
                    [&](const DynamicSchemaHandle& candidate) {
                        return candidate != nullptr &&
                               candidate->aggregate().full_name() ==
                                   required.full_name() &&
                               candidate->identity().canonical_digest() ==
                                   required.digest();
                    });
                if (owner != context->descriptor_owners.end()) {
                    closure.push_back(*owner);
                }
            }
            auto planned = LayoutPlanner::Plan(*dependency, closure);
            if (!planned.ok()) return planned.status();
            context->layouts.emplace(name, std::move(*planned));
        }
        const std::string root_name(descriptor->aggregate().full_name());
        const SchemaDescriptor* root_descriptor =
            context->FindDescriptor(root_name);
        const LayoutPlan* root_layout = context->FindLayout(root_name);
        if (root_descriptor == nullptr || root_layout == nullptr) {
            return Status::Error(StatusCode::kInternal,
                                 "dynamic root context is incomplete");
        }
        return DynamicView(std::move(context), root_descriptor, root_layout, root);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

const SchemaDescriptor& DynamicView::descriptor() const noexcept {
    return *descriptor_;
}
const LayoutPlan& DynamicView::layout() const noexcept { return *layout_; }

Result<bool> DynamicView::Has(const FieldHandle& field) const noexcept {
    auto index = ResolveHandle(*descriptor_, *layout_, field);
    return index.ok() ? HasByIndex(*index) : Result<bool>(index.status());
}
Result<bool> DynamicView::HasById(uint32_t field_id) const noexcept {
    auto field = FieldHandle::ById(*descriptor_, *layout_, field_id);
    return field.ok() ? Has(*field) : Result<bool>(field.status());
}
Result<bool> DynamicView::HasByIndex(size_t field_index) const noexcept {
    if (field_index >= layout_->fields().size()) {
        return Status::Error(StatusCode::kNotFound, "field index is out of range");
    }
    auto data = ViewData(*this);
    if (!data.ok()) return data.status();
    return IsPresent(*layout_, layout_->fields()[field_index], *data);
}

Result<int64_t> DynamicView::GetSigned(const FieldHandle& field) const noexcept {
    auto index = ResolveHandle(*descriptor_, *layout_, field);
    if (!index.ok()) return index.status();
    const auto scalar = descriptor_->aggregate().fields()[*index].type().scalar();
    if (scalar != ScalarType::kInt32 && scalar != ScalarType::kInt64) {
        return Mismatch("field is not a signed integer");
    }
    auto data = FieldData(*this, *index);
    if (!data.ok()) return data.status();
    return scalar == ScalarType::kInt32
               ? static_cast<int64_t>(static_cast<int32_t>(Read32(*data)))
               : static_cast<int64_t>(Read64(*data));
}
Result<uint64_t> DynamicView::GetUnsigned(const FieldHandle& field) const noexcept {
    auto index = ResolveHandle(*descriptor_, *layout_, field);
    if (!index.ok()) return index.status();
    const auto scalar = descriptor_->aggregate().fields()[*index].type().scalar();
    if (scalar != ScalarType::kUint32 && scalar != ScalarType::kUint64 &&
        scalar != ScalarType::kFixed32 && scalar != ScalarType::kFixed64) {
        return Mismatch("field is not an unsigned/fixed integer");
    }
    auto data = FieldData(*this, *index);
    if (!data.ok()) return data.status();
    return (scalar == ScalarType::kUint32 || scalar == ScalarType::kFixed32)
               ? static_cast<uint64_t>(Read32(*data))
               : Read64(*data);
}
Result<bool> DynamicView::GetBool(const FieldHandle& field) const noexcept {
    auto index = ResolveHandle(*descriptor_, *layout_, field);
    if (!index.ok()) return index.status();
    if (descriptor_->aggregate().fields()[*index].type().scalar() !=
        ScalarType::kBool) {
        return Mismatch("field is not bool");
    }
    auto data = FieldData(*this, *index);
    if (!data.ok()) return data.status();
    return static_cast<uint8_t>(**data) != 0;
}
Result<uint32_t> DynamicView::GetFloat32Bits(
    const FieldHandle& field) const noexcept {
    auto index = ResolveHandle(*descriptor_, *layout_, field);
    if (!index.ok()) return index.status();
    if (descriptor_->aggregate().fields()[*index].type().scalar() !=
        ScalarType::kFloat) {
        return Mismatch("field is not float");
    }
    auto data = FieldData(*this, *index);
    return data.ok() ? Result<uint32_t>(Read32(*data))
                     : Result<uint32_t>(data.status());
}
Result<uint64_t> DynamicView::GetFloat64Bits(
    const FieldHandle& field) const noexcept {
    auto index = ResolveHandle(*descriptor_, *layout_, field);
    if (!index.ok()) return index.status();
    if (descriptor_->aggregate().fields()[*index].type().scalar() !=
        ScalarType::kDouble) {
        return Mismatch("field is not double");
    }
    auto data = FieldData(*this, *index);
    return data.ok() ? Result<uint64_t>(Read64(*data))
                     : Result<uint64_t>(data.status());
}
Result<std::string_view> DynamicView::GetString(
    const FieldHandle& field) const noexcept {
    auto index = ResolveHandle(*descriptor_, *layout_, field);
    if (!index.ok()) return index.status();
    if (descriptor_->aggregate().fields()[*index].type().scalar() !=
        ScalarType::kString) {
        return Mismatch("field is not string");
    }
    auto data = FieldData(*this, *index);
    if (!data.ok()) return data.status();
    MINO_ASSIGN_OR_RETURN(size_t metadata_offset,
                          FieldStorageOffset(*this, *index));
    const auto max_bytes =
        descriptor_->aggregate().fields()[*index].constraints().max_bytes();
    if (!max_bytes.has_value() ||
        *max_bytes > std::numeric_limits<size_t>::max()) {
        return Corrupt("string descriptor has invalid max_bytes");
    }
    auto bytes = VariableBytes(*context_, storage_handle_, metadata_offset,
                               static_cast<size_t>(*max_bytes));
    if (!bytes.ok()) return bytes.status();
    if (bytes->empty()) return std::string_view{};
    return std::string_view(reinterpret_cast<const char*>(bytes->data()),
                            bytes->size());
}
Result<std::span<const std::byte>> DynamicView::GetBytes(
    const FieldHandle& field) const noexcept {
    auto index = ResolveHandle(*descriptor_, *layout_, field);
    if (!index.ok()) return index.status();
    if (descriptor_->aggregate().fields()[*index].type().scalar() !=
        ScalarType::kBytes) {
        return Mismatch("field is not bytes");
    }
    auto data = FieldData(*this, *index);
    if (!data.ok()) return data.status();
    MINO_ASSIGN_OR_RETURN(size_t metadata_offset,
                          FieldStorageOffset(*this, *index));
    const auto max_bytes =
        descriptor_->aggregate().fields()[*index].constraints().max_bytes();
    if (!max_bytes.has_value() ||
        *max_bytes > std::numeric_limits<size_t>::max()) {
        return Corrupt("bytes descriptor has invalid max_bytes");
    }
    return VariableBytes(*context_, storage_handle_, metadata_offset,
                         static_cast<size_t>(*max_bytes));
}
Result<DynamicView> DynamicView::GetNested(
    const FieldHandle& field) const noexcept {
    auto index = ResolveHandle(*descriptor_, *layout_, field);
    if (!index.ok()) return index.status();
    const TypeDescriptor& type = descriptor_->aggregate().fields()[*index].type();
    if (type.kind() != TypeDescriptor::Kind::kUserDefined) {
        return Mismatch("field is not nested");
    }
    const SchemaDescriptor* nested = context_->FindDescriptor(type.name());
    const LayoutPlan* nested_layout = context_->FindLayout(type.name());
    if (nested == nullptr || nested_layout == nullptr) {
        return Status::Error(StatusCode::kNotFound,
                             "nested descriptor/layout is unavailable");
    }
    auto data = FieldData(*this, *index);
    if (!data.ok()) return data.status();
    MINO_ASSIGN_OR_RETURN(size_t field_offset,
                          FieldStorageOffset(*this, *index));
    if (nested->aggregate().kind() == AggregateKind::kStruct) {
        return DynamicView(context_, nested, nested_layout, ShmHandle{},
                           storage_handle_, field_offset);
    }
    MINO_ASSIGN_OR_RETURN(
        VariableAccess value,
        ResolveVariable(*context_, storage_handle_, field_offset));
    if (value.element_size != 1 ||
        value.length != nested_layout->object_size() ||
        value.capacity != nested_layout->object_size()) {
        return Corrupt("nested message metadata changed");
    }
    MINO_ASSIGN_OR_RETURN(
        SlabView slab,
        InspectContextAllocation(*context_, value.handle));
    if (slab.schema_short_id != nested->identity().short_id() ||
        slab.layout_version != nested->identity().layout_version()) {
        return Corrupt("nested message schema metadata changed");
    }
    return DynamicView(context_, nested, nested_layout, value.handle);
}
Result<DynamicVectorView> DynamicView::GetVector(
    const FieldHandle& field) const noexcept {
    auto index = ResolveHandle(*descriptor_, *layout_, field);
    if (!index.ok()) return index.status();
    const FieldDescriptor& descriptor_field =
        descriptor_->aggregate().fields()[*index];
    if (descriptor_field.type().kind() != TypeDescriptor::Kind::kVector ||
        descriptor_field.type().element_type() == nullptr) {
        return Mismatch("field is not vector");
    }
    auto metadata = FieldData(*this, *index);
    if (!metadata.ok()) return metadata.status();
    MINO_ASSIGN_OR_RETURN(size_t metadata_offset,
                          FieldStorageOffset(*this, *index));
    MINO_ASSIGN_OR_RETURN(
        VariableAccess value,
        ResolveVariable(*context_, storage_handle_, metadata_offset));
    MINO_ASSIGN_OR_RETURN(
        size_t expected_element_size,
        ViewElementSize(*context_, *descriptor_field.type().element_type()));
    if (!descriptor_field.constraints().max_capacity().has_value() ||
        value.capacity > *descriptor_field.constraints().max_capacity() ||
        value.element_size != expected_element_size) {
        return Corrupt("vector metadata violates descriptor constraints");
    }
    return DynamicVectorView(context_, descriptor_field.type().element_type(),
                             &descriptor_field.constraints(), storage_handle_,
                             metadata_offset, value.length,
                             value.element_size);
}

Result<DynamicVectorView::ElementAccess> DynamicVectorView::ElementData(
    size_t index) const noexcept {
    if (index >= size_) {
        return Status::Error(StatusCode::kNotFound,
                             "vector element index is out of range");
    }
    auto value = ResolveVariable(*context_, metadata_handle_, metadata_offset_);
    if (!value.ok()) return value.status();
    if (value->length != size_ || value->element_size != element_size_ ||
        (constraints_->max_capacity().has_value() &&
         value->capacity > *constraints_->max_capacity())) {
        return Corrupt("vector metadata changed after view creation");
    }
    size_t element_offset = 0;
    size_t element_end = 0;
    size_t child_bytes = 0;
    if (!CheckedMultiply(value->capacity, value->element_size, child_bytes) ||
        !CheckedMultiply(index, element_size_, element_offset) ||
        !CheckedAdd(element_offset, element_size_, element_end) ||
        element_end > child_bytes) {
        return Corrupt("vector element offset overflows child bounds");
    }
    return ElementAccess{.data = value->data + element_offset,
                         .storage_handle = value->handle,
                         .storage_offset = element_offset};
}

Result<int64_t> DynamicVectorView::GetSigned(size_t index) const noexcept {
    if (element_type_->scalar() != ScalarType::kInt32 &&
        element_type_->scalar() != ScalarType::kInt64) {
        return Mismatch("vector element is not signed integer");
    }
    auto data = ElementData(index);
    if (!data.ok()) return data.status();
    return element_type_->scalar() == ScalarType::kInt32
               ? static_cast<int64_t>(
                     static_cast<int32_t>(Read32(data->data)))
               : static_cast<int64_t>(Read64(data->data));
}
Result<uint64_t> DynamicVectorView::GetUnsigned(size_t index) const noexcept {
    const auto scalar = element_type_->scalar();
    if (scalar != ScalarType::kUint32 && scalar != ScalarType::kUint64 &&
        scalar != ScalarType::kFixed32 && scalar != ScalarType::kFixed64) {
        return Mismatch("vector element is not unsigned/fixed integer");
    }
    auto data = ElementData(index);
    if (!data.ok()) return data.status();
    return scalar == ScalarType::kUint32 || scalar == ScalarType::kFixed32
               ? static_cast<uint64_t>(Read32(data->data))
               : Read64(data->data);
}
Result<bool> DynamicVectorView::GetBool(size_t index) const noexcept {
    if (element_type_->scalar() != ScalarType::kBool) {
        return Mismatch("vector element is not bool");
    }
    auto data = ElementData(index);
    return data.ok()
               ? Result<bool>(static_cast<uint8_t>(*data->data) != 0)
               : Result<bool>(data.status());
}
Result<uint32_t> DynamicVectorView::GetFloat32Bits(size_t index) const noexcept {
    if (element_type_->scalar() != ScalarType::kFloat) {
        return Mismatch("vector element is not float");
    }
    auto data = ElementData(index);
    return data.ok() ? Result<uint32_t>(Read32(data->data))
                     : Result<uint32_t>(data.status());
}
Result<uint64_t> DynamicVectorView::GetFloat64Bits(size_t index) const noexcept {
    if (element_type_->scalar() != ScalarType::kDouble) {
        return Mismatch("vector element is not double");
    }
    auto data = ElementData(index);
    return data.ok() ? Result<uint64_t>(Read64(data->data))
                     : Result<uint64_t>(data.status());
}
Result<std::string_view> DynamicVectorView::GetString(size_t index) const noexcept {
    if (element_type_->scalar() != ScalarType::kString) {
        return Mismatch("vector element is not string");
    }
    auto data = ElementData(index);
    if (!data.ok()) return data.status();
    if (!constraints_->max_bytes().has_value() ||
        *constraints_->max_bytes() > std::numeric_limits<size_t>::max()) {
        return Corrupt("vector string descriptor has invalid max_bytes");
    }
    auto bytes = VariableBytes(
        *context_, data->storage_handle, data->storage_offset,
        static_cast<size_t>(*constraints_->max_bytes()));
    if (!bytes.ok()) return bytes.status();
    if (bytes->empty()) return std::string_view{};
    return std::string_view(reinterpret_cast<const char*>(bytes->data()),
                            bytes->size());
}
Result<std::span<const std::byte>> DynamicVectorView::GetBytes(
    size_t index) const noexcept {
    if (element_type_->scalar() != ScalarType::kBytes) {
        return Mismatch("vector element is not bytes");
    }
    auto data = ElementData(index);
    if (!data.ok()) return data.status();
    if (!constraints_->max_bytes().has_value() ||
        *constraints_->max_bytes() > std::numeric_limits<size_t>::max()) {
        return Corrupt("vector bytes descriptor has invalid max_bytes");
    }
    return VariableBytes(*context_, data->storage_handle,
                         data->storage_offset,
                         static_cast<size_t>(*constraints_->max_bytes()));
}
Result<DynamicView> DynamicVectorView::GetNested(size_t index) const noexcept {
    if (element_type_->kind() != TypeDescriptor::Kind::kUserDefined) {
        return Mismatch("vector element is not nested");
    }
    auto data = ElementData(index);
    if (!data.ok()) return data.status();
    const SchemaDescriptor* nested = context_->FindDescriptor(element_type_->name());
    const LayoutPlan* layout = context_->FindLayout(element_type_->name());
    if (nested == nullptr || layout == nullptr) {
        return Status::Error(StatusCode::kNotFound,
                             "nested vector descriptor/layout is unavailable");
    }
    if (nested->aggregate().kind() == AggregateKind::kStruct) {
        return DynamicView(context_, nested, layout, ShmHandle{},
                           data->storage_handle, data->storage_offset);
    }
    MINO_ASSIGN_OR_RETURN(
        VariableAccess value,
        ResolveVariable(*context_, data->storage_handle,
                        data->storage_offset));
    if (value.element_size != 1 || value.length != layout->object_size() ||
        value.capacity != layout->object_size()) {
        return Corrupt("nested vector message metadata changed");
    }
    MINO_ASSIGN_OR_RETURN(
        SlabView slab,
        InspectContextAllocation(*context_, value.handle));
    if (slab.schema_short_id != nested->identity().short_id() ||
        slab.layout_version != nested->identity().layout_version()) {
        return Corrupt("nested vector message schema changed");
    }
    return DynamicView(context_, nested, layout, value.handle);
}
Result<DynamicVectorView> DynamicVectorView::GetVector(size_t index) const noexcept {
    if (element_type_->kind() != TypeDescriptor::Kind::kVector ||
        element_type_->element_type() == nullptr) {
        return Mismatch("vector element is not vector");
    }
    auto metadata = ElementData(index);
    if (!metadata.ok()) return metadata.status();
    MINO_ASSIGN_OR_RETURN(
        VariableAccess value,
        ResolveVariable(*context_, metadata->storage_handle,
                        metadata->storage_offset));
    MINO_ASSIGN_OR_RETURN(
        size_t expected_element_size,
        ViewElementSize(*context_, *element_type_->element_type()));
    if (!constraints_->max_capacity().has_value() ||
        value.capacity > *constraints_->max_capacity() ||
        value.element_size != expected_element_size) {
        return Corrupt("nested vector metadata violates constraints");
    }
    return DynamicVectorView(context_, element_type_->element_type(), constraints_,
                             metadata->storage_handle,
                             metadata->storage_offset, value.length,
                             value.element_size);
}

Result<DynamicVector> DynamicVectorView::ToDynamicVector() const noexcept {
    try {
        DynamicVector result;
        for (size_t i = 0; i < size_; ++i) {
            DynamicValue value = DynamicValue::Unsigned(0);
            if (element_type_->kind() == TypeDescriptor::Kind::kUserDefined) {
                auto nested = GetNested(i);
                if (!nested.ok()) return nested.status();
                auto message = nested->ToDynamicMessage();
                if (!message.ok()) return message.status();
                auto dynamic = DynamicValue::Message(
                    std::make_shared<DynamicMessage>(std::move(*message)));
                if (!dynamic.ok()) return dynamic.status();
                value = std::move(*dynamic);
            } else if (element_type_->kind() == TypeDescriptor::Kind::kVector) {
                auto nested = GetVector(i);
                if (!nested.ok()) return nested.status();
                auto vector = nested->ToDynamicVector();
                if (!vector.ok()) return vector.status();
                auto dynamic = DynamicValue::Vector(
                    std::make_shared<DynamicVector>(std::move(*vector)));
                if (!dynamic.ok()) return dynamic.status();
                value = std::move(*dynamic);
            } else {
                switch (*element_type_->scalar()) {
                    case ScalarType::kInt32:
                    case ScalarType::kInt64: {
                        auto v = GetSigned(i); if (!v.ok()) return v.status();
                        value = DynamicValue::Signed(*v); break;
                    }
                    case ScalarType::kUint32:
                    case ScalarType::kUint64:
                    case ScalarType::kFixed32:
                    case ScalarType::kFixed64: {
                        auto v = GetUnsigned(i); if (!v.ok()) return v.status();
                        value = DynamicValue::Unsigned(*v); break;
                    }
                    case ScalarType::kFloat: {
                        auto v = GetFloat32Bits(i); if (!v.ok()) return v.status();
                        value = DynamicValue::Float32Bits(*v); break;
                    }
                    case ScalarType::kDouble: {
                        auto v = GetFloat64Bits(i); if (!v.ok()) return v.status();
                        value = DynamicValue::Float64Bits(*v); break;
                    }
                    case ScalarType::kBool: {
                        auto v = GetBool(i); if (!v.ok()) return v.status();
                        value = DynamicValue::Boolean(*v); break;
                    }
                    case ScalarType::kString: {
                        auto v = GetString(i); if (!v.ok()) return v.status();
                        auto dynamic = DynamicValue::String(*v);
                        if (!dynamic.ok()) return dynamic.status();
                        value = std::move(*dynamic); break;
                    }
                    case ScalarType::kBytes: {
                        auto v = GetBytes(i); if (!v.ok()) return v.status();
                        auto dynamic = DynamicValue::Bytes(*v);
                        if (!dynamic.ok()) return dynamic.status();
                        value = std::move(*dynamic); break;
                    }
                }
            }
            MINO_RETURN_IF_ERROR(result.Add(std::move(value)));
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Result<DynamicMessage> DynamicView::ToDynamicMessage() const noexcept {
    try {
        DynamicMessage result(context_->options.unknown_fields);
        const auto fields = descriptor_->aggregate().fields();
        for (size_t i = 0; i < fields.size(); ++i) {
            auto present = HasByIndex(i);
            if (!present.ok()) return present.status();
            if (!*present) continue;
            auto handle = FieldHandle::ByIndex(*descriptor_, *layout_, i);
            if (!handle.ok()) return handle.status();
            DynamicValue value = DynamicValue::Unsigned(0);
            const TypeDescriptor& type = fields[i].type();
            if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
                auto nested = GetNested(*handle);
                if (!nested.ok()) return nested.status();
                auto message = nested->ToDynamicMessage();
                if (!message.ok()) return message.status();
                auto dynamic = DynamicValue::Message(
                    std::make_shared<DynamicMessage>(std::move(*message)));
                if (!dynamic.ok()) return dynamic.status();
                value = std::move(*dynamic);
            } else if (type.kind() == TypeDescriptor::Kind::kVector) {
                auto vector = GetVector(*handle);
                if (!vector.ok()) return vector.status();
                auto converted = vector->ToDynamicVector();
                if (!converted.ok()) return converted.status();
                auto dynamic = DynamicValue::Vector(
                    std::make_shared<DynamicVector>(std::move(*converted)));
                if (!dynamic.ok()) return dynamic.status();
                value = std::move(*dynamic);
            } else {
                switch (*type.scalar()) {
                    case ScalarType::kInt32:
                    case ScalarType::kInt64: {
                        auto v = GetSigned(*handle); if (!v.ok()) return v.status();
                        value = DynamicValue::Signed(*v); break;
                    }
                    case ScalarType::kUint32:
                    case ScalarType::kUint64:
                    case ScalarType::kFixed32:
                    case ScalarType::kFixed64: {
                        auto v = GetUnsigned(*handle); if (!v.ok()) return v.status();
                        value = DynamicValue::Unsigned(*v); break;
                    }
                    case ScalarType::kFloat: {
                        auto v = GetFloat32Bits(*handle); if (!v.ok()) return v.status();
                        value = DynamicValue::Float32Bits(*v); break;
                    }
                    case ScalarType::kDouble: {
                        auto v = GetFloat64Bits(*handle); if (!v.ok()) return v.status();
                        value = DynamicValue::Float64Bits(*v); break;
                    }
                    case ScalarType::kBool: {
                        auto v = GetBool(*handle); if (!v.ok()) return v.status();
                        value = DynamicValue::Boolean(*v); break;
                    }
                    case ScalarType::kString: {
                        auto v = GetString(*handle); if (!v.ok()) return v.status();
                        auto dynamic = DynamicValue::String(*v);
                        if (!dynamic.ok()) return dynamic.status();
                        value = std::move(*dynamic); break;
                    }
                    case ScalarType::kBytes: {
                        auto v = GetBytes(*handle); if (!v.ok()) return v.status();
                        auto dynamic = DynamicValue::Bytes(*v);
                        if (!dynamic.ok()) return dynamic.status();
                        value = std::move(*dynamic); break;
                    }
                }
            }
            MINO_RETURN_IF_ERROR(result.SetField(fields[i].id(), std::move(value)));
        }
        if (layout_->unknown_fields_offset().has_value()) {
            MINO_ASSIGN_OR_RETURN(const std::byte* object, ViewData(*this));
            size_t metadata_offset = 0;
            if (!CheckedAdd(storage_offset_, *layout_->unknown_fields_offset(),
                            metadata_offset)) {
                return Corrupt("unknown field metadata offset overflows");
            }
            (void)metadata_offset;
            const std::byte* metadata =
                object + *layout_->unknown_fields_offset();
            const ShmHandle handle = ReadHandle(metadata);
            const uint64_t field_count = Read64(
                metadata + VariableMetadataLayout::kLengthOffset);
            const uint64_t payload_size = Read64(
                metadata + VariableMetadataLayout::kCapacityOffset);
            const uint64_t element_size = Read64(
                metadata + VariableMetadataLayout::kElementSizeOffset);
            size_t framing_bytes = 0;
            size_t payload_limit = 0;
            if (!CheckedMultiply(context_->options.unknown_fields.max_fields, 8,
                                 framing_bytes) ||
                !CheckedAdd(context_->options.unknown_fields.max_bytes,
                            framing_bytes, payload_limit) ||
                field_count > context_->options.unknown_fields.max_fields ||
                payload_size > payload_limit || element_size != 0) {
                return Corrupt("unknown field metadata exceeds configured limits");
            }
            if (field_count == 0) {
                if (!handle.IsNull() || payload_size != 0) {
                    return Corrupt("empty unknown field set has child metadata");
                }
            } else {
                MINO_ASSIGN_OR_RETURN(
                    SlabView slab,
                    InspectContextAllocation(*context_, handle));
                if (slab.object_size != payload_size) {
                    return Corrupt("unknown field child slab is invalid");
                }
                const auto* cursor = static_cast<const std::byte*>(slab.data);
                const std::byte* end = cursor + slab.object_size;
                size_t canonical_bytes = 0;
                for (uint64_t i = 0; i < field_count; ++i) {
                    if (end - cursor < 8) {
                        return Corrupt("unknown field framing is truncated");
                    }
                    const uint32_t field_id = Read32(cursor);
                    const uint32_t bytes = Read32(cursor + 4);
                    cursor += 8;
                    if (static_cast<size_t>(end - cursor) < bytes ||
                        canonical_bytes >
                            context_->options.unknown_fields.max_bytes ||
                        bytes > context_->options.unknown_fields.max_bytes -
                                    canonical_bytes) {
                        return Corrupt("unknown field payload exceeds limits");
                    }
                    canonical_bytes += bytes;
                    MINO_RETURN_IF_ERROR(result.mutable_unknown_fields().Add(
                        field_id, std::span(cursor, bytes)));
                    cursor += bytes;
                }
                if (cursor != end) {
                    return Corrupt("unknown field child has trailing bytes");
                }
            }
        }
        return result;
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

}  // namespace mino::schema
