// Copyright 2026 The Mino Authors
//
// Licensed under the GNU Lesser General Public License, Version 3.0.

#include "mino/schema/object_graph_walker.h"

#include <algorithm>
#include <limits>
#include <map>
#include <new>
#include <set>
#include <string>
#include <tuple>
#include <utility>

#include "mino/common/status.h"
#include "mino/schema/canonical.h"
#include "mino/schema/descriptor_closure.h"
#include "mino/shm/allocator/slab_header.h"

namespace mino::schema {
namespace {

Status Corrupt(std::string_view message) {
    return Status::Error(StatusCode::kCorruption, message);
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

ShmHandle ReadHandle(const std::byte* data) noexcept {
    return ShmHandle{.offset = Read64(data),
                     .generation = Read32(data + 8),
                     .region_id = Read32(data + 12)};
}

bool IsZero(std::span<const std::byte> bytes) noexcept {
    for (std::byte byte : bytes) {
        if (byte != std::byte{0}) return false;
    }
    return true;
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

bool DecodeCanonicalVarint(std::span<const std::byte> bytes, size_t& offset,
                           uint64_t& value) noexcept {
    value = 0;
    const size_t begin = offset;
    for (size_t i = 0; i < 10; ++i) {
        if (offset >= bytes.size()) return false;
        const uint8_t byte = static_cast<uint8_t>(bytes[offset++]);
        if (i == 9 && (byte & 0xfeu) != 0) return false;
        value |= static_cast<uint64_t>(byte & 0x7fu) << (7 * i);
        if ((byte & 0x80u) == 0) {
            size_t minimal = 1;
            uint64_t copy = value;
            while (copy >= 0x80u) {
                copy >>= 7;
                ++minimal;
            }
            return offset - begin == minimal;
        }
    }
    return false;
}

bool ValidateCanonicalUnknown(uint32_t expected_field_id,
                              std::span<const std::byte> frame) noexcept {
    size_t offset = 0;
    uint64_t tag = 0;
    if (!DecodeCanonicalVarint(frame, offset, tag) || tag == 0 ||
        (tag >> 3) != expected_field_id) {
        return false;
    }
    switch (tag & 7u) {
        case 0: {
            uint64_t ignored = 0;
            return DecodeCanonicalVarint(frame, offset, ignored) &&
                   offset == frame.size();
        }
        case 1:
            return frame.size() - offset == 8;
        case 2: {
            uint64_t length = 0;
            return DecodeCanonicalVarint(frame, offset, length) &&
                   length == frame.size() - offset;
        }
        case 5:
            return frame.size() - offset == 4;
        default:
            return false;
    }
}

bool CheckedMultiply(uint64_t lhs, uint64_t rhs, uint64_t& result) noexcept {
    if (lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs) {
        return false;
    }
    result = lhs * rhs;
    return true;
}

struct Shape {
    size_t size = 0;
    FieldStorageKind storage = FieldStorageKind::kScalar;
};

class Walker {
public:
    Walker(const SchemaDescriptor& root, const LayoutPlan& root_layout,
           const CentralSlabAllocator& allocator,
           std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors,
           const ObjectGraphWalkOptions& options)
        : allocator_(allocator), options_(options) {
        AddDescriptor(root);
        layouts_.emplace(std::string(root.aggregate().full_name()), root_layout);
        for (const auto& descriptor : descriptors) {
            if (descriptor != nullptr) AddDescriptor(*descriptor);
        }
    }

    Result<std::vector<ShmHandle>> Run(const SchemaDescriptor& descriptor,
                                       const LayoutPlan& layout,
                                       ShmHandle root) {
        if (!setup_status_.ok()) return setup_status_;
        if (root.IsNull()) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "dynamic root handle is null");
        }
        MINO_RETURN_IF_ERROR(VisitAllocation(root));
        auto slab = allocator_.Inspect(root);
        if (!slab.ok()) return slab.status();
        MINO_RETURN_IF_ERROR(CheckState(*slab));
        MINO_RETURN_IF_ERROR(CheckSlab(*slab, descriptor, layout.object_size()));
        if (slab->owner_epoch == 0 ||
            slab->allocation_transaction_id == 0 ||
            slab->allocation_flags != kAllocationFlagTransactionRoot) {
            return Corrupt("dynamic root has invalid transaction ownership");
        }
        root_owner_epoch_ = slab->owner_epoch;
        root_transaction_id_ = slab->allocation_transaction_id;
        MINO_RETURN_IF_ERROR(WalkMessage(descriptor, layout,
                                         static_cast<const std::byte*>(slab->data),
                                         0));
        return handles_;
    }

private:
    void AddDescriptor(const SchemaDescriptor& descriptor) {
        if (!setup_status_.ok()) return;
        if (DigestShortId(descriptor.identity().canonical_digest()) !=
            descriptor.identity().short_id()) {
            setup_status_ = Status::Error(
                StatusCode::kSchemaMismatch,
                "dynamic descriptor short ID does not match full digest");
            return;
        }
        for (const auto& [name, existing] : descriptors_) {
            if ((existing->identity().short_id() ==
                     descriptor.identity().short_id() &&
                 existing->identity().canonical_digest() !=
                     descriptor.identity().canonical_digest()) ||
                (name == descriptor.aggregate().full_name() &&
                 existing->identity().canonical_digest() !=
                     descriptor.identity().canonical_digest())) {
                setup_status_ = Status::Error(
                    StatusCode::kSchemaMismatch,
                    "dynamic descriptor closure contains an identity collision");
                return;
            }
        }
        descriptors_.insert_or_assign(
            std::string(descriptor.aggregate().full_name()), &descriptor);
    }

    Status CheckState(const SlabView& slab) const {
        if (slab.state == ObjectState::kPublished) return Status::Ok();
        if (options_.allow_building && slab.state == ObjectState::kBuilding) {
            return Status::Ok();
        }
        return Corrupt("dynamic graph contains a slab in an invalid state");
    }

    Status CheckSlab(const SlabView& slab, const SchemaDescriptor& descriptor,
                     size_t expected_size) const {
        if (slab.object_size != expected_size ||
            slab.schema_short_id != descriptor.identity().short_id() ||
            slab.layout_version != descriptor.identity().layout_version()) {
            return Corrupt("dynamic slab schema/layout/size mismatch");
        }
        return Status::Ok();
    }

    Status VisitAllocation(ShmHandle handle) {
        if (handles_.size() >= options_.limits.max_allocations) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "dynamic graph allocation limit exceeded");
        }
        const auto key = std::tuple(handle.region_id, handle.offset,
                                    handle.generation);
        if (!seen_.insert(key).second) {
            return Corrupt("dynamic object graph contains shared ownership or a cycle");
        }
        handles_.push_back(handle);
        return Status::Ok();
    }

    Result<const SchemaDescriptor*> Descriptor(std::string_view name) const {
        const auto it = descriptors_.find(name);
        if (it == descriptors_.end()) {
            return Status::Error(StatusCode::kNotFound,
                                 "dynamic descriptor dependency is unavailable");
        }
        return it->second;
    }

    Result<const LayoutPlan*> Plan(const SchemaDescriptor& descriptor) {
        const std::string name(descriptor.aggregate().full_name());
        const auto found = layouts_.find(name);
        if (found != layouts_.end()) return &found->second;
        std::vector<std::shared_ptr<const SchemaDescriptor>> closure;
        closure.reserve(descriptor.dependencies().size());
        for (const DependencyDescriptor& dependency :
             descriptor.dependencies()) {
            const auto candidate = descriptors_.find(dependency.full_name());
            if (candidate != descriptors_.end() &&
                candidate->second->identity().canonical_digest() ==
                    dependency.digest()) {
                closure.emplace_back(std::shared_ptr<const SchemaDescriptor>(
                    std::shared_ptr<const SchemaDescriptor>{},
                    candidate->second));
            }
        }
        auto planned = LayoutPlanner::Plan(descriptor, closure);
        if (!planned.ok()) return planned.status();
        auto [it, inserted] = layouts_.emplace(name, std::move(*planned));
        (void)inserted;
        return &it->second;
    }

    Result<Shape> TypeShape(const TypeDescriptor& type,
                            const ConstraintSet& constraints) {
        (void)constraints;
        if (type.kind() == TypeDescriptor::Kind::kScalar) {
            if (!type.scalar().has_value()) return Corrupt("incomplete scalar type");
            switch (*type.scalar()) {
                case ScalarType::kBool:
                    return Shape{1, FieldStorageKind::kScalar};
                case ScalarType::kInt32:
                case ScalarType::kUint32:
                case ScalarType::kFixed32:
                case ScalarType::kFloat:
                    return Shape{4, FieldStorageKind::kScalar};
                case ScalarType::kInt64:
                case ScalarType::kUint64:
                case ScalarType::kFixed64:
                case ScalarType::kDouble:
                    return Shape{8, FieldStorageKind::kScalar};
                case ScalarType::kString:
                case ScalarType::kBytes:
                    return Shape{VariableMetadataLayout::kSize,
                                 FieldStorageKind::kVariable};
            }
        }
        if (type.kind() == TypeDescriptor::Kind::kVector) {
            return Shape{VariableMetadataLayout::kSize,
                         FieldStorageKind::kVariable};
        }
        MINO_ASSIGN_OR_RETURN(const SchemaDescriptor* descriptor,
                              Descriptor(type.name()));
        MINO_ASSIGN_OR_RETURN(const LayoutPlan* plan, Plan(*descriptor));
        if (descriptor->aggregate().kind() == AggregateKind::kStruct) {
            return Shape{plan->object_size(), FieldStorageKind::kInlineStruct};
        }
        return Shape{VariableMetadataLayout::kSize,
                     FieldStorageKind::kVariable};
    }

    Status ValidateHeader(const SchemaDescriptor& descriptor,
                          const LayoutPlan& layout,
                          const std::byte* data) const {
        if (layout.header_size() != ObjectHeaderLayout::kSize ||
            layout.presence_bitmap_offset() != ObjectHeaderLayout::kSize ||
            Read32(data + ObjectHeaderLayout::kLayoutVersionOffset) !=
                layout.layout_version() ||
            Read32(data + ObjectHeaderLayout::kHeaderSizeOffset) !=
                layout.header_size() ||
            Read64(data + ObjectHeaderLayout::kSchemaShortIdOffset) !=
                descriptor.identity().short_id() ||
            Read64(data + ObjectHeaderLayout::kObjectSizeOffset) !=
                layout.fixed_area_size() ||
            Read32(data + ObjectHeaderLayout::kFieldCountOffset) !=
                descriptor.aggregate().fields().size() ||
            Read32(data + ObjectHeaderLayout::kPresenceBitmapWordsOffset) !=
                layout.presence_bitmap_words()) {
            return Corrupt("dynamic object header does not match LayoutPlan");
        }
        const size_t optional_bits = layout.presence_bitmap_words() * 64;
        size_t used_bits = 0;
        for (const FieldLayout& field : layout.fields()) {
            if (field.presence_bit().has_value()) {
                used_bits = std::max(used_bits, *field.presence_bit() + 1);
            }
        }
        for (size_t bit = used_bits; bit < optional_bits; ++bit) {
            const uint64_t word = Read64(
                data + layout.presence_bitmap_offset() + (bit / 64) * 8);
            if ((word & (uint64_t{1} << (bit % 64))) != 0) {
                return Corrupt("dynamic object has non-zero unused presence bits");
            }
        }
        return Status::Ok();
    }

    bool Present(const FieldLayout& field, const LayoutPlan& layout,
                 const std::byte* data) const noexcept {
        if (!field.presence_bit().has_value()) return true;
        const size_t bit = *field.presence_bit();
        const uint64_t word = Read64(
            data + layout.presence_bitmap_offset() + (bit / 64) * 8);
        return (word & (uint64_t{1} << (bit % 64))) != 0;
    }

    Status WalkMessage(const SchemaDescriptor& descriptor,
                       const LayoutPlan& layout, const std::byte* data,
                       size_t depth) {
        struct DescriptorGuard {
            const SchemaDescriptor*& slot;
            const SchemaDescriptor* saved;
            ~DescriptorGuard() { slot = saved; }
        } guard{current_descriptor_, current_descriptor_};
        current_descriptor_ = &descriptor;
        if (depth > options_.limits.max_depth) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "dynamic graph depth limit exceeded");
        }
        MINO_RETURN_IF_ERROR(ValidateHeader(descriptor, layout, data));
        const auto fields = descriptor.aggregate().fields();
        const auto field_layouts = layout.fields();
        if (fields.size() != field_layouts.size()) {
            return Corrupt("descriptor and layout field counts differ");
        }
        for (size_t i = 0; i < fields.size(); ++i) {
            const FieldDescriptor& field = fields[i];
            const FieldLayout& field_layout = field_layouts[i];
            if (field.id() != field_layout.field_id() ||
                field_layout.offset() > layout.object_size() ||
                field_layout.size() > layout.object_size() - field_layout.offset()) {
                return Corrupt("dynamic field layout is inconsistent");
            }
            const std::byte* field_data = data + field_layout.offset();
            if (!Present(field_layout, layout, data)) {
                if (!IsZero(std::span(field_data, field_layout.size()))) {
                    return Corrupt("absent optional field has non-zero storage");
                }
                continue;
            }
            MINO_RETURN_IF_ERROR(WalkValue(field.type(), field.constraints(),
                                           field_data, depth + 1));
        }
        if (layout.unknown_fields_offset().has_value()) {
            if (*layout.unknown_fields_offset() > layout.object_size() ||
                VariableMetadataLayout::kSize >
                    layout.object_size() - *layout.unknown_fields_offset()) {
                return Corrupt("unknown field metadata is out of object bounds");
            }
            MINO_RETURN_IF_ERROR(WalkUnknownFields(
                data + *layout.unknown_fields_offset(), depth + 1));
        } else if (descriptor.aggregate().kind() == AggregateKind::kMessage) {
            return Corrupt("message layout is missing unknown field metadata");
        }
        return Status::Ok();
    }

    Status WalkUnknownFields(const std::byte* metadata, size_t depth) {
        if (depth > options_.limits.max_depth) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "unknown field graph depth limit exceeded");
        }
        const ShmHandle handle = ReadHandle(metadata);
        const uint64_t field_count =
            Read64(metadata + VariableMetadataLayout::kLengthOffset);
        const uint64_t payload_size =
            Read64(metadata + VariableMetadataLayout::kCapacityOffset);
        const uint64_t element_size =
            Read64(metadata + VariableMetadataLayout::kElementSizeOffset);
        uint64_t configured_payload_limit = 0;
        if (options_.unknown_fields.max_fields >
                std::numeric_limits<uint64_t>::max() / 8 ||
            options_.unknown_fields.max_bytes >
                std::numeric_limits<uint64_t>::max() -
                    static_cast<uint64_t>(options_.unknown_fields.max_fields) * 8) {
            return Status::Error(StatusCode::kInvalidArgument,
                                 "unknown field limits overflow");
        }
        configured_payload_limit =
            static_cast<uint64_t>(options_.unknown_fields.max_bytes) +
            static_cast<uint64_t>(options_.unknown_fields.max_fields) * 8;
        if (element_size != 0 || field_count > kDynamicUnknownFieldMaxCount ||
            field_count > options_.unknown_fields.max_fields ||
            payload_size > kDynamicUnknownFieldMaxBytes ||
            payload_size > configured_payload_limit) {
            return Corrupt("unknown field metadata violates configured limits");
        }
        if (field_count == 0) {
            return handle.IsNull() && payload_size == 0
                       ? Status::Ok()
                       : Corrupt("empty unknown field set has child storage");
        }
        if (handle.IsNull() || current_descriptor_ == nullptr) {
            return Corrupt("non-empty unknown field set has no child handle");
        }
        MINO_ASSIGN_OR_RETURN(
            SlabView slab,
            InspectChild(handle, payload_size,
                         current_descriptor_->identity().short_id(),
                         current_descriptor_->identity().layout_version()));
        const auto* cursor = static_cast<const std::byte*>(slab.data);
        const std::byte* end = cursor + payload_size;
        uint64_t canonical_bytes = 0;
        for (uint64_t i = 0; i < field_count; ++i) {
            if (end - cursor < 8) {
                return Corrupt("unknown field framing is truncated");
            }
            const uint32_t field_id = Read32(cursor);
            const uint32_t bytes = Read32(cursor + 4);
            cursor += 8;
            if (field_id == 0 || bytes == 0 ||
                static_cast<size_t>(end - cursor) < bytes ||
                canonical_bytes > options_.unknown_fields.max_bytes ||
                bytes > options_.unknown_fields.max_bytes - canonical_bytes ||
                !ValidateCanonicalUnknown(field_id,
                                          std::span(cursor, bytes))) {
                return Corrupt("unknown field frame is invalid");
            }
            canonical_bytes += bytes;
            cursor += bytes;
        }
        return cursor == end
                   ? Status::Ok()
                   : Corrupt("unknown field child has trailing bytes");
    }

    Status WalkValue(const TypeDescriptor& type,
                     const ConstraintSet& constraints, const std::byte* data,
                     size_t depth) {
        if (depth > options_.limits.max_depth) {
            return Status::Error(StatusCode::kResourceExhausted,
                                 "dynamic graph depth limit exceeded");
        }
        MINO_ASSIGN_OR_RETURN(Shape shape, TypeShape(type, constraints));
        if (shape.storage == FieldStorageKind::kScalar) {
            if (type.scalar() == ScalarType::kBool &&
                static_cast<uint8_t>(*data) > 1) {
                return Corrupt("dynamic bool is not encoded as 0 or 1");
            }
            return Status::Ok();
        }
        if (shape.storage == FieldStorageKind::kInlineStruct) {
            MINO_ASSIGN_OR_RETURN(const SchemaDescriptor* descriptor,
                                  Descriptor(type.name()));
            MINO_ASSIGN_OR_RETURN(const LayoutPlan* plan, Plan(*descriptor));
            return WalkMessage(*descriptor, *plan, data, depth);
        }
        return WalkVariable(type, constraints, data, depth);
    }

    Result<SlabView> InspectChild(ShmHandle handle,
                                  uint64_t expected_bytes,
                                  uint64_t schema_short_id,
                                  uint32_t layout_version) {
        MINO_RETURN_IF_ERROR(VisitAllocation(handle));
        auto slab = allocator_.Inspect(handle);
        if (!slab.ok()) return slab.status();
        MINO_RETURN_IF_ERROR(CheckState(*slab));
        if (expected_bytes > std::numeric_limits<uint32_t>::max() ||
            slab->object_size != expected_bytes ||
            slab->schema_short_id != schema_short_id ||
            slab->layout_version != layout_version ||
            slab->owner_epoch != root_owner_epoch_ ||
            slab->allocation_transaction_id != root_transaction_id_ ||
            slab->allocation_flags != kAllocationFlagTransactionChild) {
            return Corrupt("dynamic child slab metadata/ownership mismatch");
        }
        return *slab;
    }

    Status WalkVariable(const TypeDescriptor& type,
                        const ConstraintSet& constraints,
                        const std::byte* metadata, size_t depth) {
        const ShmHandle handle = ReadHandle(
            metadata + VariableMetadataLayout::kHandleOffset);
        const uint64_t length = Read64(
            metadata + VariableMetadataLayout::kLengthOffset);
        const uint64_t capacity = Read64(
            metadata + VariableMetadataLayout::kCapacityOffset);
        const uint64_t element_size = Read64(
            metadata + VariableMetadataLayout::kElementSizeOffset);
        if (length > capacity) {
            return Corrupt("dynamic metadata length exceeds capacity");
        }

        if (type.kind() == TypeDescriptor::Kind::kScalar) {
            if (!type.scalar().has_value() ||
                (*type.scalar() != ScalarType::kString &&
                 *type.scalar() != ScalarType::kBytes)) {
                return Corrupt("scalar has unexpected variable metadata");
            }
            if (element_size != 1 ||
                !constraints.max_bytes().has_value() ||
                capacity > *constraints.max_bytes()) {
                return Corrupt("dynamic string/bytes metadata violates constraints");
            }
            if (handle.IsNull()) {
                return length == 0 && capacity == 0
                           ? Status::Ok()
                           : Corrupt("non-empty dynamic bytes has a null handle");
            }
            MINO_ASSIGN_OR_RETURN(
                SlabView slab,
                InspectChild(handle, capacity, current_descriptor_->identity().short_id(),
                             current_descriptor_->identity().layout_version()));
            const auto bytes = std::span(
                static_cast<const std::byte*>(slab.data),
                static_cast<size_t>(length));
            if (*type.scalar() == ScalarType::kString && !IsValidUtf8(bytes)) {
                return Corrupt("dynamic string is not valid UTF-8");
            }
            const auto unused = std::span(
                static_cast<const std::byte*>(slab.data) + length,
                static_cast<size_t>(capacity - length));
            return IsZero(unused)
                       ? Status::Ok()
                       : Corrupt("unused dynamic bytes capacity is non-zero");
        }

        if (type.kind() == TypeDescriptor::Kind::kUserDefined) {
            MINO_ASSIGN_OR_RETURN(const SchemaDescriptor* descriptor,
                                  Descriptor(type.name()));
            MINO_ASSIGN_OR_RETURN(const LayoutPlan* plan, Plan(*descriptor));
            if (descriptor->aggregate().kind() == AggregateKind::kStruct) {
                return Corrupt("inline struct unexpectedly uses metadata");
            }
            if (handle.IsNull() || element_size != 1 ||
                length != plan->object_size() || capacity != plan->object_size()) {
                return Corrupt("nested message metadata is invalid");
            }
            MINO_ASSIGN_OR_RETURN(
                SlabView slab,
                InspectChild(handle, plan->object_size(),
                             descriptor->identity().short_id(),
                             descriptor->identity().layout_version()));
            return WalkMessage(*descriptor, *plan,
                               static_cast<const std::byte*>(slab.data), depth);
        }

        if (type.element_type() == nullptr ||
            !constraints.max_capacity().has_value()) {
            return Corrupt("vector metadata has an incomplete descriptor");
        }
        MINO_ASSIGN_OR_RETURN(Shape element,
                              TypeShape(*type.element_type(), constraints));
        if (element_size != element.size || capacity > *constraints.max_capacity()) {
            return Corrupt("vector metadata violates element size/capacity");
        }
        uint64_t bytes = 0;
        if (!CheckedMultiply(capacity, element_size, bytes)) {
            return Corrupt("vector capacity byte size overflows");
        }
        if (handle.IsNull()) {
            return length == 0 && capacity == 0
                       ? Status::Ok()
                       : Corrupt("non-empty vector has a null handle");
        }
        MINO_ASSIGN_OR_RETURN(
            SlabView slab,
            InspectChild(handle, bytes, current_descriptor_->identity().short_id(),
                         current_descriptor_->identity().layout_version()));
        const std::byte* elements = static_cast<const std::byte*>(slab.data);
        for (uint64_t i = 0; i < length; ++i) {
            MINO_RETURN_IF_ERROR(WalkValue(*type.element_type(), constraints,
                                           elements + i * element_size,
                                           depth + 1));
        }
        const uint64_t used_bytes = length * element_size;
        const auto unused = std::span(
            elements + used_bytes, static_cast<size_t>(bytes - used_bytes));
        return IsZero(unused)
                   ? Status::Ok()
                   : Corrupt("unused vector capacity is non-zero");
    }

    const CentralSlabAllocator& allocator_;
    const ObjectGraphWalkOptions& options_;
    std::map<std::string, const SchemaDescriptor*, std::less<>> descriptors_;
    std::map<std::string, LayoutPlan, std::less<>> layouts_;
    std::set<std::tuple<uint32_t, uint64_t, uint32_t>> seen_;
    std::vector<ShmHandle> handles_;
    const SchemaDescriptor* current_descriptor_ = nullptr;
    uint64_t root_owner_epoch_ = 0;
    uint64_t root_transaction_id_ = 0;
    Status setup_status_ = Status::Ok();

public:
    void SetCurrentDescriptor(const SchemaDescriptor* descriptor) noexcept {
        current_descriptor_ = descriptor;
    }
};

}  // namespace

Result<std::vector<ShmHandle>> ObjectGraphWalker::Collect(
    const SchemaDescriptor& descriptor, const LayoutPlan& layout,
    ShmHandle root, const CentralSlabAllocator& allocator,
    std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors,
    const ObjectGraphWalkOptions& options) noexcept {
    try {
        MINO_RETURN_IF_ERROR(ValidateDescriptorClosure(descriptor, descriptors));
        MINO_RETURN_IF_ERROR(
            LayoutPlanner::Validate(descriptor, layout, descriptors));
        Walker walker(descriptor, layout, allocator, descriptors, options);
        walker.SetCurrentDescriptor(&descriptor);
        return walker.Run(descriptor, layout, root);
    } catch (const std::bad_alloc&) {
        return Status::Error(StatusCode::kResourceExhausted);
    } catch (...) {
        return Status::Error(StatusCode::kInternal);
    }
}

Status ObjectGraphWalker::Validate(
    const SchemaDescriptor& descriptor, const LayoutPlan& layout,
    ShmHandle root, const CentralSlabAllocator& allocator,
    std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors,
    const ObjectGraphWalkOptions& options) noexcept {
    auto collected = Collect(descriptor, layout, root, allocator, descriptors,
                             options);
    return collected.ok() ? Status::Ok() : collected.status();
}

Status ObjectGraphWalker::Reclaim(
    const SchemaDescriptor& descriptor, const LayoutPlan& layout,
    ShmHandle root, CentralSlabAllocator& allocator,
    std::span<const std::shared_ptr<const SchemaDescriptor>> descriptors,
    const ObjectGraphLimits& limits) noexcept {
    ObjectGraphWalkOptions options;
    options.limits = limits;
    options.allow_building = true;
    auto collected = Collect(descriptor, layout, root, allocator, descriptors,
                             options);
    if (!collected.ok()) return collected.status();
    for (ShmHandle handle : *collected) {
        auto slab = allocator.Inspect(handle);
        if (!slab.ok()) return slab.status();
        if (slab->state != ObjectState::kAllocated &&
            slab->state != ObjectState::kBuilding) {
            return Status::Error(
                StatusCode::kPermissionDenied,
                "published dynamic graphs require Pin-aware lifecycle ownership");
        }
    }
    for (auto it = collected->rbegin(); it != collected->rend(); ++it) {
        MINO_RETURN_IF_ERROR(allocator.Abort(*it));
    }
    return Status::Ok();
}

}  // namespace mino::schema
